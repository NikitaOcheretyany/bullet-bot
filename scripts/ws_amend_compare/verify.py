#!/usr/bin/env python3
"""Repeat final reconciliation using public read-only GETs; never sends orders."""
import json
import sys
import time
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

path = Path(sys.argv[1])
events = [json.loads(line) for line in (path / 'events.jsonl').read_text().splitlines()]
original = next(e for e in reversed(events) if e['event'] == 'verification')
start = next(e for e in events if e['event'] == 'start')
address = original['trades'][0]['address']
raw = {}
for name, route, params in [
    ('orders', '/fapi/v1/allOrders', dict(address=address, symbol='BTC-USD', limit=1000)),
    ('trades', '/fapi/v1/userTrades', dict(address=address, symbol='BTC-USD', limit=1000, isDegen='false')),
    ('open_orders', '/fapi/v1/openOrders', dict(address=address, symbol='BTC-USD')),
    ('account', '/fapi/v3/account', dict(address=address)),
]:
    url = 'https://tradingapi.bullet.xyz' + route + '?' + urlencode(params)
    begin = time.time_ns()
    request = Request(url, headers={'User-Agent': 'bullet-rust-sdk', 'Accept': 'application/json'})
    with urlopen(request, timeout=15) as response:
        body = json.load(response)
    raw[name] = dict(method='GET', url=url, begin_wall_ns=begin, end_wall_ns=time.time_ns(), body=body)
    (path / 'verification-recheck-raw.json').write_text(json.dumps(raw, indent=2) + '\n')

ids = set(original['resting_ids'] + original['filled_ids'])
v = {**original,
     'orders': [o for o in raw['orders']['body']['data'] if o['orderId'] in ids],
     'trades': [t for t in raw['trades']['body']['data'] if t['orderId'] in ids],
     'btc_open_orders_after': len(raw['open_orders']['body']),
     'btc_position_after': str(sum((Decimal(p['positionAmt']) for p in raw['account']['body']['positions']
                                   if p['symbol'] == 'BTC-USD'), Decimal(0))),
     'rechecked_wall_ns': time.time_ns()}
orders = {o['orderId']: o for o in v['orders']}
quantity = Decimal(start['quantity'])
valid = v['btc_open_orders_after'] == 0 and Decimal(v['btc_position_after']) == 0 \
    and len(orders) == start.get('expected_orders', start['pairs'] * 5) \
    and len(v['filled_ids']) == start.get('expected_filled', start['pairs'] * 4) \
    and all(oid in orders and orders[oid]['status'] == 'CANCELED' and Decimal(orders[oid]['executedQty']) == 0
            for oid in v['resting_ids']) \
    and all(oid in orders and orders[oid]['status'] == 'FILLED' and Decimal(orders[oid]['executedQty']) == quantity
            for oid in v['filled_ids']) \
    and all(any(t['orderId'] == oid for t in v['trades']) for oid in v['filled_ids']) \
    and all(not t['maker'] for t in v['trades']) and v['series_error'] is None and v['cleanup_error'] is None
result = dict(complete=valid, verification=v)
(path / 'verification-recheck.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(dict(complete=valid, position=v['btc_position_after'], open_orders=v['btc_open_orders_after'],
                     historical_orders=len(orders), trades=len(v['trades']))))
raise SystemExit(0 if valid else 1)
