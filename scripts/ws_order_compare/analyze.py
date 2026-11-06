#!/usr/bin/env python3
"""Read captured probe data only; never contacts an exchange."""
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

def stats(values):
    a=sorted(values)
    if not a:return {'n':0,'p50':None,'p95':None,'p99':None,'max':None}
    def q(p):
        x=(len(a)-1)*p;i=int(x)
        return a[i]+(a[min(i+1,len(a)-1)]-a[i])*(x-i)
    return dict(n=len(a),p50=q(.5),p95=q(.95),p99=q(.99),max=a[-1])

def analyze(directory):
    p=Path(directory)
    events=[];bad_lines=[]
    for i,line in enumerate((p/'events.jsonl').read_text().splitlines(),1):
        try:events.append(json.loads(line))
        except json.JSONDecodeError:bad_lines.append(i)
    starts=[r for r in events if r['event']=='start']
    if not starts:
        raise ValueError('No start record; inspect stderr.log for preflight failure')
    start=starts[0]
    clocks=[]
    for e in events:
        if e['event']!='clock':continue
        a,b=e['begin'],e['received'];server=e['server_us']*1000
        if abs(server-a['wall_ns'])>60_000_000_000:
            raise ValueError('Clock sample differs by >60 s: check units or synchronization')
        clocks.append(dict(phase=e['phase'],mid_wall_ns=(a['wall_ns']+b['wall_ns'])//2,
            offset_ms=(2*server-a['wall_ns']-b['wall_ns'])/2e6,
            rtt_ms=(b['mono_ns']-a['mono_ns'])/1e6,
            offset_interval_ms=[(server-b['wall_ns'])/1e6,(server-a['wall_ns'])/1e6]))
    best=[min([c for c in clocks if c['phase']==phase],key=lambda c:c['rtt_ms'])
          for phase in ['before','after'] if any(c['phase']==phase for c in clocks)]
    def offset_at(wall_ns):
        if not best:return None
        if len(best)==1:return best[0]['offset_ms']
        lo,hi=best
        weight=max(0,min(1,(wall_ns-lo['mid_wall_ns'])/(hi['mid_wall_ns']-lo['mid_wall_ns'])))
        return lo['offset_ms']*(1-weight)+hi['offset_ms']*weight
    verifications=[e for e in events if e['event']=='verification']
    verification=verifications[-1] if verifications else {}
    history={str(r['orderId']):r for r in verification.get('orders',[])}
    observations=[];problems=[]
    for e in events:
        if e['event']!='operation':continue
        s=e['send_start'];frames=e['frames'];r=e['receipts']
        o=dict(pair=e['pair'],order_type=e['order_type'],operation=e['operation'],
            client_id=str(e['client_id']),request_id=str(e['request_id']),order_id=str(r['order_id']) if r['order_id'] is not None else None,
            send_wall_ns=s['wall_ns'],send_mono_ns=s['mono_ns'],
            prepare_ms=(s['mono_ns']-e['prepare_start']['mono_ns'])/1e6,
            send_flush_ms=(e['send_done']['mono_ns']-s['mono_ns'])/1e6,
            ack_ok=r['ack_ok'],unexpected_fill=r['unexpected_fill'],error=e['error'])
        if r['ack'] is not None:
            ack=frames[r['ack']]
            o['send_to_ack_ms']=(ack['received']['mono_ns']-s['mono_ns'])/1e6
            o['ack_status']=ack['message'].get('results',{}).get('status')
        if r['private'] is not None:
            f=frames[r['private']];raw=f['message'];venue=raw['o']['T'];published=raw['E']
            o.update(venue_T_us=venue,private_E_us=published,private_status=raw['o']['X'],
                send_to_private_ms=(f['received']['mono_ns']-s['mono_ns'])/1e6,
                send_to_venue_uncorrected_ms=(venue*1000-s['wall_ns'])/1e6,
                venue_to_private_E_ms=(published-venue)/1000,
                venue_to_private_receive_uncorrected_ms=(f['received']['wall_ns']-venue*1000)/1e6)
            offset=offset_at(s['wall_ns']);o['api_clock_offset_estimate_ms']=offset
            if offset is not None:o['send_to_venue_estimate_ms']=o['send_to_venue_uncorrected_ms']-offset
            h=history.get(o['order_id'])
            field='time' if o['operation']=='place' else 'updateTime'
            o['history_T_matches']=h is not None and int(h[field])==venue
            if not o['history_T_matches']:problems.append(f"History T mismatch/missing: {o['client_id']} {o['operation']}")
        o['complete']=r['ack_ok'] and r['private'] is not None and not e['error'] and not r['unexpected_fill']
        if not o['complete']:problems.append(f"Incomplete operation: {o['client_id']} {o['operation']}")
        observations.append(o)
    metrics=[]
    metric_names=['send_to_venue_estimate_ms','send_to_venue_uncorrected_ms','send_to_ack_ms',
        'send_to_private_ms','venue_to_private_E_ms','venue_to_private_receive_uncorrected_ms','prepare_ms','send_flush_ms']
    for kind in ['PostOnly','Limit']:
        for op in ['place','cancel']:
            group=[o for o in observations if o['order_type']==kind and o['operation']==op]
            for name in metric_names:
                # Do not silently substitute missing/private/history timestamps or trim slow ACKs.
                rows=[o for o in group if name in o and (not name.startswith('send_to_venue') or o.get('history_T_matches'))]
                metrics.append(dict(order_type=kind,operation=op,metric=name,**stats(o[name] for o in rows)))
    pairs=[]
    for pair in sorted({o['pair'] for o in observations}):
        obs={o['order_type']:o for o in observations if o['pair']==pair and o['operation']=='place'}
        if len(obs)!=2:continue
        a,b=obs['PostOnly'],obs['Limit']
        if a.get('history_T_matches') and b.get('history_T_matches'):
            pairs.append(dict(pair=pair,limit_minus_postonly_venue_ms=b['send_to_venue_uncorrected_ms']-a['send_to_venue_uncorrected_ms'],
                limit_minus_postonly_ack_ms=b['send_to_ack_ms']-a['send_to_ack_ms']))
    expected=int(start['pairs'])*4
    complete=any(e['event']=='complete' for e in events) and len(observations)==expected and not problems and not bad_lines
    if not start['live']:
        complete=any(e['event']=='check_complete' and e.get('mutations')==0 for e in events) and not bad_lines
    summary=dict(live=start['live'],complete=complete,started=start,operations=len(observations),
        expected_operations=expected,clock_samples=clocks,best_clock_samples=best,
        clock_caveat='Midpoint correction assumes API clock agrees with event clock. RTT/2 is not a proven event-clock accuracy bound. Uncorrected times retained. Pair differences cancel a constant event-clock offset.',
        metrics=metrics,paired_difference=stats(r['limit_minus_postonly_venue_ms'] for r in pairs),
        problems=problems,bad_json_lines=bad_lines,verification=verification)
    (p/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    def save_csv(name,rows):
        if not rows:return
        with (p/name).open('w') as f:
            w=csv.DictWriter(f,fieldnames=sorted(set().union(*(r.keys() for r in rows))));w.writeheader();w.writerows(rows)
    save_csv('observations.csv',observations);save_csv('percentiles.csv',metrics);save_csv('paired.csv',pairs)
    if not start['live']:
        (p/'CHECK.md').write_text(('Read-only preflight passed.' if complete else 'Read-only preflight incomplete.')+' No orders placed or cancelled. Clock and ping observations only.\n')
        return summary
    when=datetime.fromtimestamp(start['at']['wall_ns']/1e9,ZoneInfo('Europe/Moscow')).isoformat()
    md=['# WS: PostOnly, далёкие Limit и отмены','',f'Начало: {when}. Статус полного измерения: {complete}.',
        '',f'BTC-USD; {start["quantity"]} BTC; по 30 заявок каждого типа, одинаковая цена в каждой паре, 10% ниже bid. Один WS, чередование AB/BA. Одна заявка в рынке за раз.','',
        'Начало всех трёх основных интервалов — `send_start`, непосредственно перед WS send после подписи и кодирования. Это вызов отправки, не ingress биржи. ACK и private recv — после разбора SDK, по одним локальным монотонным часам. Биржевое событие — `o.T` private NEW/CANCELED в микросекундах, сверенное с `allOrders.time/updateTime`.','',
        '**Главная оценка — отправка → событие на бирже.** Из исходной разности часов вычтена оценка offset по API `/time` до/после серии (минимальный RTT, линейная интерполяция). Она предполагает одинаковые часы API и исполнения; это предположение не проверено. Значение не отделяет сеть от обработки биржей. В CSV сохранена также разность без поправки.','',
        'При N=30 p99 почти равен максимуму выборки; надёжной оценки редкого хвоста нет. Квантили линейные `(N−1)×p`, выбросы не удаляются. Пропущенные подтверждения не считаются нулём.','']
    labels={'send_to_venue_estimate_ms':'Отправка → биржевое событие, оценка*','send_to_ack_ms':'Отправка → ACK','send_to_private_ms':'Отправка → private NEW/CANCELED'}
    def fmt(x):return '—' if x is None else f'{x:.3f}'
    md+=['| Тип | Операция | Интервал | N | p50, мс | p95 | p99 | max |','|---|---|---|---:|---:|---:|---:|---:|']
    for r in metrics:
        if r['metric'] in labels:md.append('| '+' | '.join([r['order_type'],r['operation'],labels[r['metric']],str(r['n']),*[fmt(r[k]) for k in ['p50','p95','p99','max']]])+' |')
    md+=['','## Калибровка часов','']
    for c in best:md.append(f'- {c["phase"]}: API−local ≈ {c["offset_ms"]:.3f} мс, RTT {c["rtt_ms"]:.3f} мс. Половина RTT не является гарантией точности часов биржи.')
    md+=['','## Проверки','',f'- Операций: {len(observations)}/{expected}; пар с обеими биржевыми метками: {len(pairs)}.',f'- Проблемы: {problems or "нет"}; повреждённые строки: {bad_lines or "нет"}.',
        f'- Финальная BTC-позиция: {verification.get("btc_position_after", "не измерена")}; открытые BTC-ордера: {verification.get("btc_open_orders_after", "не измерены")}.',
        '- При проблемах серии таблица описывает только полученные наблюдения; полный тест не объявляется успешным.',
        '- `o.T` — биржевая метка транзакции. API не раскрывает точку её установки относительно всех стадий matching engine; точное физическое время принятия/снятия риска из этих данных не доказать.',
        '- Приход private означает, что сообщение получил измеритель. Дополнительная доставка до обработчика стратегии через adapter/event bus/mutex здесь не измеряется.',
        '- Фактическое исполнение сделки не измеряется: планируется постановка и отмена далёких заявок без сделок. Также не разделяются исходящая сеть, очередь сервера и работа matching engine.',
        '', '[Смысл полей и единицы WS](https://tradingapi.bullet.xyz/docs/ws/).',
        '', 'Исходные данные: events.jsonl; индивидуальные измерения: observations.csv; все квантили: percentiles.csv; парные разности: paired.csv.']
    (p/'REPORT.md').write_text('\n'.join(md)+'\n')
    return summary

if __name__=='__main__':
    try:
        result=analyze(sys.argv[1])
    except Exception as exc:
        print(f'Отчёт не построен: {exc}',file=sys.stderr)
        raise SystemExit(2) from None
    print(json.dumps({k:result[k] for k in ['live','complete','operations','problems']},ensure_ascii=False))
