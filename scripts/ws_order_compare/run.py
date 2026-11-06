#!/usr/bin/env python3
"""User entrypoint. Defaults to read-only; --live executes the bounded plan."""
import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tomllib
from datetime import datetime, timezone
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
parser=argparse.ArgumentParser()
parser.add_argument('--live',action='store_true',help='Place/cancel 30 PostOnly and 30 Limit orders on mainnet')
args=parser.parse_args()
os.umask(0o077)
binary=ROOT/'target/debug/examples/ws_order_compare'
source=ROOT/'crates/exchanges/bullet/examples/ws_order_compare.rs'
if not binary.is_file() or source.stat().st_mtime>binary.stat().st_mtime:
    print('Собираю актуальную версию измерителя…',flush=True)
    built=subprocess.run(['cargo','build','-p','bb-exchange-bullet','--example','ws_order_compare'],cwd=ROOT)
    if built.returncode:raise SystemExit('Сборка не прошла; торговый тест не запускался')
config=ROOT/'data/runs/lit-live-20261028T125505Z.toml'
env=os.environ.copy()
if not env.get('BB_BULLET_KEY_FILE'):
    saved=tomllib.loads(config.read_text())
    venue=next(v for v in saved['exchanges'].values() if v.get('type')=='bullet')
    env['BB_BULLET_KEY_FILE']=venue['key_file']
if not Path(env['BB_BULLET_KEY_FILE']).is_file():raise SystemExit('Configured key file unavailable')
# Avoid concurrent measurements/trading by the local bot. No process is stopped.
for pid in Path('/proc').iterdir():
    if not pid.name.isdigit() or int(pid.name)==os.getpid():continue
    try:name=(pid/'comm').read_text().strip()
    except (OSError,PermissionError):continue
    if name in {'bb-bot','order_latency','ws_order_compar'}:
        raise SystemExit('Another bot/probe is active; no test started')
prefix='ws-order-compare' if args.live else 'ws-order-compare-check'
out=ROOT/'data/runs'/f'{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{os.getpid()}'
out.mkdir(parents=True)
for name in ['binary','source']:
    path=binary if name=='binary' else source
    digest=hashlib.sha256(path.read_bytes()).hexdigest()
    (out/(name+'-sha256.txt')).write_text(digest+'\n')
def clock_capture(name):
    try:r=subprocess.run(['chronyc','tracking'],capture_output=True,text=True,timeout=3)
    except (OSError,subprocess.TimeoutExpired):return
    (out/name).write_text(r.stdout+r.stderr)
clock_capture('chrony-before.txt')
print('Результаты:',out,flush=True)
print('60 постановок + 60 адресных отмен; BTC, минимальный объём, 10% ниже bid.' if args.live else 'Только чтение счёта, подписка и часы; торговых запросов нет.',flush=True)
with (out/'events.jsonl').open('w',buffering=1) as log,(out/'stderr.log').open('w',buffering=1) as err:
    proc=subprocess.Popen([str(binary),'--live' if args.live else '--check'],cwd=ROOT,env=env,stdout=log,stderr=subprocess.PIPE,text=True)
    try:
        assert proc.stderr is not None
        for line in proc.stderr:
            err.write(line)
            print(line,end='',flush=True)
        code=proc.wait()
    except KeyboardInterrupt:
        print('Останавливаю после отмены текущей тестовой заявки…',flush=True)
        proc.send_signal(signal.SIGINT)
        code=proc.wait()
clock_capture('chrony-after.txt')
(out/'exit.json').write_text(json.dumps({'exit_code':code,'live_requested':args.live})+'\n')
analysis=subprocess.run([sys.executable,str(Path(__file__).with_name('analyze.py')),str(out)],cwd=ROOT)
if code or analysis.returncode:
    print('Серия не подтверждена полностью. Диагностика:',out/'stderr.log')
    if (out/'summary.json').is_file():print('Частичные результаты:',out/'summary.json')
    raise SystemExit(code or analysis.returncode)
print('Готово:',out/('REPORT.md' if args.live else 'CHECK.md'))
