#!/usr/bin/env python3
"""Offline, reproducible analysis. Reads the probe's JSONL; no exchange access."""
import csv
import json
import sys
from decimal import Decimal
from pathlib import Path


def stats(values):
    a = sorted(values)
    if not a:
        return dict(n=0, p50=None, p95=None, p99=None, max=None)
    def q(p):
        h = (len(a) - 1) * p
        i = int(h)
        return a[i] + (a[min(i + 1, len(a) - 1)] - a[i]) * (h - i)
    return dict(n=len(a), min=a[0], p50=q(.5), p95=q(.95), p99=q(.99), max=a[-1])


def write_csv(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(k for row in rows for k in row))
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)


def analyze(directory):
    path = Path(directory)
    events = [json.loads(line) for line in (path / 'events.jsonl').read_text().splitlines()]
    start = next(e for e in events if e['event'] == 'start')
    planned_arms = start.get('arms', ['AmendLimit', 'IOC'])
    verification = next((e for e in reversed(events) if e['event'] == 'verification'), {})
    recheck_path = path / 'verification-recheck.json'
    recheck = json.loads(recheck_path.read_text()) if recheck_path.exists() else {}
    if recheck:
        verification = recheck['verification']
    clocks = []
    for e in events:
        if e['event'] != 'clock':
            continue
        a, b = e['begin'], e['received']
        clocks.append(dict(phase=e['phase'], mid_wall_ns=(a['wall_ns'] + b['wall_ns']) // 2,
                           offset_ms=(2 * e['server_us'] * 1000 - a['wall_ns'] - b['wall_ns']) / 2e6,
                           rtt_ms=(b['mono_ns'] - a['mono_ns']) / 1e6))
    best = [min((c for c in clocks if c['phase'] == phase), key=lambda c: c['rtt_ms'])
            for phase in ('before', 'after') if any(c['phase'] == phase for c in clocks)]
    def offset_at(wall_ns):
        if len(best) != 2:
            return None
        lo, hi = best
        w = max(0, min(1, (wall_ns - lo['mid_wall_ns']) / (hi['mid_wall_ns'] - lo['mid_wall_ns'])))
        return lo['offset_ms'] * (1 - w) + hi['offset_ms'] * w

    trades = {int(t['id']): t for t in verification.get('trades', [])}
    history = {int(o['orderId']): o for o in verification.get('orders', [])}
    public = {}
    for e in events:
        frames = e.get('frames', [])
        if 'frame' in e:
            frames = frames + [e['frame']]
        for f in frames:
            msg = f['message']
            if msg.get('e') == 'aggTrade':
                public[int(msg['a'])] = msg

    observations, issues = [], []
    entry_fills = set()
    for e in events:
        if e['event'] != 'operation':
            continue
        s, state, frames = e['send_start'], e['outcome'], e['frames']
        row = dict(pair=e['pair'], arm=e['arm'], role=e['role'], method=e['method'],
                   client_id=e['client_id'], order_id=state['order_id'],
                   send_wall_ns=s['wall_ns'], send_mono_ns=s['mono_ns'],
                   prepare_ms=(s['mono_ns'] - e['prepare_start']['mono_ns']) / 1e6,
                   send_flush_ms=(e['send_done']['mono_ns'] - s['mono_ns']) / 1e6,
                   quantity=e['quantity'], filled=state['filled'], maker_fill=state['maker_fill'],
                   ack_ok=state['ack_ok'], error=e['error'])
        if state['ack'] is not None:
            ack = frames[state['ack']]
            row['send_to_ack_ms'] = (ack['received']['mono_ns'] - s['mono_ns']) / 1e6
            row['ack_status'] = ack['message'].get('results', {}).get('status')
        fills = {}
        for f in frames:
            o = f['message'].get('o', {})
            if o.get('x') == 'TRADE' and o.get('i') == state['order_id']:
                fills.setdefault(int(o['t']), f)
        if fills:
            first_t = min(f['message']['o']['T'] for f in fills.values())
            last_t = max(f['message']['o']['T'] for f in fills.values())
            first_recv = min(fills.values(), key=lambda f: f['received']['mono_ns'])
            pub = first_recv['message']['E']
            offset = offset_at(s['wall_ns'])
            raw_ms = (first_t * 1000 - s['wall_ns']) / 1e6
            row.update(venue_first_fill_us=first_t, venue_last_fill_us=last_t,
                       send_to_venue_uncorrected_ms=raw_ms,
                       api_clock_offset_ms=offset,
                       send_to_private_fill_ms=(first_recv['received']['mono_ns'] - s['mono_ns']) / 1e6,
                       first_to_last_venue_fill_ms=(last_t - first_t) / 1000,
                       venue_to_publication_ms=(pub - first_recv['message']['o']['T']) / 1000)
            if offset is not None:
                row['send_to_venue_estimate_ms'] = raw_ms - offset
                row['send_to_full_fill_estimate_ms'] = (last_t * 1000 - s['wall_ns']) / 1e6 - offset
                row['venue_to_private_estimate_ms'] = (
                    first_recv['received']['wall_ns'] - first_recv['message']['o']['T'] * 1000
                ) / 1e6 + offset
            history_ok = True
            public_ok = True
            for tid, f in fills.items():
                o = f['message']['o']
                t = trades.get(tid)
                matched = t is not None and int(t['orderId']) == state['order_id'] and int(t['time']) == o['T'] \
                    and Decimal(t['price']) == Decimal(o['L']) and Decimal(t['qty']) == Decimal(o['l']) \
                    and t['maker'] is False
                history_ok &= matched
                p = public.get(tid)
                matched_public = p is not None and p['T'] == o['T'] \
                    and Decimal(p['p']) == Decimal(o['L']) and Decimal(p['q']) == Decimal(o['l'])
                public_ok &= matched_public
                if e['role'] == 'entry':
                    entry_fills.add(tid)
            row['rest_fill_matches'] = history_ok
            row['public_fill_matches'] = public_ok
            if not history_ok:
                issues.append(f"REST fill mismatch/missing for {e['client_id']}")
            h = history.get(state['order_id'])
            row['history_filled'] = h is not None and h['status'] == 'FILLED' \
                and Decimal(h['executedQty']) == Decimal(e['quantity'])
        row['complete'] = e['error'] is None and state['ack_ok'] and (
            state['first_new'] is not None and Decimal(state['filled']) == 0 if e['role'] == 'prepare'
            else Decimal(state['filled']) == Decimal(e['quantity']) and state['terminal'] is not None
                 and not state['maker_fill'] and row.get('rest_fill_matches', False)
                 and row.get('history_filled', False))
        if not row['complete']:
            issues.append(f"Incomplete {e['role']} {e['arm']} pair {e['pair']}")
        observations.append(row)

    entry = [r for r in observations if r['role'] == 'entry']
    metric_names = ['send_to_venue_estimate_ms', 'send_to_venue_uncorrected_ms',
                    'send_to_full_fill_estimate_ms', 'send_to_ack_ms', 'send_to_private_fill_ms',
                    'venue_to_private_estimate_ms', 'venue_to_publication_ms', 'prepare_ms', 'send_flush_ms']
    metrics = {arm: {name: stats([r[name] for r in entry if r['arm'] == arm and name in r])
                     for name in metric_names} for arm in planned_arms}
    paired = []
    for pair in range(start['pairs']):
        arms = {r['arm']: r for r in entry if r['pair'] == pair}
        if set(arms) != set(planned_arms):
            issues.append(f'Missing paired entry {pair}')
            continue
        if len(planned_arms) == 1:
            continue
        row = dict(pair=pair)
        for name in metric_names:
            if all(name in r for r in arms.values()):
                row[name + '_amend_minus_ioc'] = arms['AmendLimit'][name] - arms['IOC'][name]
        paired.append(row)
    if verification.get('series_error'):
        issues.append(verification['series_error'])
    if verification.get('cleanup_error'):
        issues.append(verification['cleanup_error'])
    if Decimal(verification.get('btc_position_after', 'NaN')) != 0 or verification.get('btc_open_orders_after') != 0:
        issues.append('Final flat/empty account not confirmed')
    if len(entry) != start['pairs'] * len(planned_arms):
        issues.append('Entry sample count differs from planned count')
    if len(history) != start.get('expected_orders', start['pairs'] * 5):
        issues.append('Historical order count differs from planned count')
    for oid in verification.get('resting_ids', []):
        h = history.get(oid)
        if h is None or h['status'] != 'CANCELED' or Decimal(h['executedQty']) != 0:
            issues.append(f'Resting PostOnly {oid} cancellation not reconciled in REST history')
    summary = dict(
        start=start, complete=(any(e['event'] == 'complete' for e in events) or recheck.get('complete', False)) and not issues,
        original_probe_exit_code=json.loads((path / 'exit.json').read_text())['exit_code'],
        read_only_recheck_complete=recheck.get('complete'),
        problems=issues, entry_metrics_ms=metrics, clock_calibration=best,
        close_metrics_ms={name: stats([r[name] for r in observations if r['role'] == 'close' and name in r])
                          for name in metric_names},
        paired_amend_minus_ioc_ms={name: stats([r[name + '_amend_minus_ioc'] for r in paired
                                              if name + '_amend_minus_ioc' in r]) for name in metric_names},
        entry_orders=len(entry), operations=len(observations),
        entry_public_matches=sum(r.get('public_fill_matches', False) for r in entry),
        entry_rest_matches=sum(r.get('rest_fill_matches', False) for r in entry),
        first_last_fill_same_time=all(r.get('first_to_last_venue_fill_ms') == 0 for r in entry),
        final_position=verification.get('btc_position_after'), final_open_orders=verification.get('btc_open_orders_after'),
        rest_trade_count=len(trades),
        realized_pnl=str(sum((Decimal(t['realizedPnl']) for t in trades.values()), Decimal(0))),
        fees=str(sum((Decimal(t['commission']) for t in trades.values()), Decimal(0))),
        quantile_method='linear interpolation at (N-1)*p; no outlier trimming',
        limitations=[
            f"N={start['pairs']} per arm: p99 is close to the sample maximum, not a stable rare-tail estimate.",
            'Venue estimate subtracts interpolated API /time midpoint offset using minimum RTT samples before/after.',
            'API clock and execution clock agreement, and symmetric network delay, are assumptions, not verified facts.',
            'Start is immediately before WS send, after signing/encoding; not exchange ingress.',
            'Only entry fills are compared. Resting PostOnly preparation and reduce-only closes are excluded.',
            'Raw SDK probe; receive timing excludes production strategy actor dispatch.',
        ],
    )
    (path / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    write_csv(path / 'observations.csv', observations)
    write_csv(path / 'paired.csv', paired)
    write_csv(path / 'percentiles.csv', [dict(arm=arm, metric=name, **v)
              for arm, group in metrics.items() for name, v in group.items()])
    return summary


if __name__ == '__main__':
    result = analyze(sys.argv[1])
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(0 if result['complete'] else 1)
