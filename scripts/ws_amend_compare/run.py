#!/usr/bin/env python3
"""Run the explicitly bounded probe. --check (default) never trades."""
import argparse
import hashlib
import json
import os
import signal
import shutil
import subprocess
import tomllib
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--ioc-only', action='store_true', help='Only minimum-size BTC IOC entries and reduce-only closes')
    parser.add_argument('--samples', type=int, choices=range(1, 21), default=20)
    args = parser.parse_args()
    os.umask(0o077)
    env = os.environ.copy()
    if not env.get('BB_BULLET_KEY_FILE'):
        config = tomllib.loads((ROOT / 'data/runs/lit-live-20261028T125505Z.toml').read_text())
        venue = next(v for v in config['exchanges'].values() if v.get('type') == 'bullet')
        env['BB_BULLET_KEY_FILE'] = venue['key_file']
    if not Path(env['BB_BULLET_KEY_FILE']).is_file():
        raise SystemExit('Configured key file unavailable')
    for pid in Path('/proc').iterdir():
        if not pid.name.isdigit() or int(pid.name) == os.getpid():
            continue
        try:
            name = (pid / 'comm').read_text().strip()
        except OSError:
            continue
        if name in {'bb-bot', 'order_latency', 'ws_order_compar', 'ws_amend_compar'}:
            raise SystemExit('Another bot/probe is active; no test started')
    binary = ROOT / 'target/debug/examples/ws_amend_compare'
    source = ROOT / 'crates/exchanges/bullet/examples/ws_amend_compare.rs'
    if not binary.is_file() or source.stat().st_mtime > binary.stat().st_mtime:
        subprocess.run(['cargo', 'build', '-p', 'bb-exchange-bullet', '--example', 'ws_amend_compare'], cwd=ROOT, check=True)
    prefix = 'ws-market-latency' if args.ioc_only else 'ws-amend-compare'
    if not args.live:
        prefix += '-check'
    out = ROOT / 'data/runs' / f'{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{os.getpid()}'
    out.mkdir(parents=True)
    (out / 'provenance.json').write_text(json.dumps({
        'source': str(source), 'source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
        'binary': str(binary), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
    }, indent=2) + '\n')
    for artifact in [source, Path(__file__), Path(__file__).with_name('analyze.py'), Path(__file__).with_name('verify.py')]:
        shutil.copy2(artifact, out / artifact.name)
    def capture_clock(name):
        try:
            result = subprocess.run(['chronyc', 'tracking'], capture_output=True, text=True, timeout=3)
            (out / name).write_text(result.stdout + result.stderr)
        except (OSError, subprocess.TimeoutExpired):
            pass
    capture_clock('chrony-before.txt')
    print('Results:', out, flush=True)
    with (out / 'events.jsonl').open('w', buffering=1) as log, (out / 'stderr.log').open('w', buffering=1) as err:
        command = [str(binary), '--live' if args.live else '--check', '--samples', str(args.samples)]
        if args.ioc_only:
            command.append('--ioc-only')
        proc = subprocess.Popen(command, cwd=ROOT,
                                env=env, stdout=log, stderr=subprocess.PIPE, text=True)
        try:
            for line in proc.stderr:
                err.write(line)
                print(line, end='', flush=True)
            code = proc.wait()
        except KeyboardInterrupt:
            print('Stopping new entries and cleaning up the test position...', flush=True)
            proc.send_signal(signal.SIGINT)
            code = proc.wait()
    (out / 'exit.json').write_text(json.dumps({'exit_code': code, 'live': args.live}) + '\n')
    capture_clock('chrony-after.txt')
    print('Exit:', code, 'Data:', out, flush=True)
    raise SystemExit(code)


if __name__ == '__main__':
    main()
