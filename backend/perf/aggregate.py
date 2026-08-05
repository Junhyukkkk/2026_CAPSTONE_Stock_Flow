"""반복 측정 결과를 조건별 중앙값으로 집계한다.

bench.sh 가 results/ 에 남긴 스냅샷을 읽는다.
반복 측정은 라벨에 _r1, _r2 ... 접미사를 붙여 실행한다.

    ./bench.sh baseline_r1 ...
    ./bench.sh baseline_r2 ...
    python3 aggregate.py baseline tuned
"""
import glob
import os
import re
import statistics
import sys

SP = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'results')
STAGES = ['realtime.total', 'redis.prev_close_get', 'redis.set_latest', 'redis.publish',
          'redis.set_publish_pipelined', 'snapshot.serialize', 'ws.dispatch',
          'storage.idempotency_check', 'storage.tx_total', 'storage.db_insert',
          'storage.instrument_registry', 'storage.idempotency_mark']


def parse(path):
    stages, cmds, scalars = {}, {}, {}
    for l in open(path).read().splitlines():
        m = re.match(r'stockflow_stage_seconds_(count|sum)\{application="[^"]*",stage="([^"]+)"\} (.*)', l)
        if m:
            stages.setdefault(m.group(2), {})[m.group(1)] = float(m.group(3))
            continue
        m = re.match(r'lettuce_command_completion_seconds_count\{.*command="([A-Z]+)".*\} (.*)', l)
        if m:
            cmds[m.group(1)] = cmds.get(m.group(1), 0) + float(m.group(2))
            continue
        m = re.match(r'(\w+)(\{[^}]*\})? ([-\d.eE+]+)$', l)
        if m and not l.startswith('#'):
            scalars.setdefault(m.group(1), float(m.group(3)))
    return stages, cmds, scalars


def run_metrics(label):
    sa, ca, va = parse(f'{SP}/snapA_{label}.txt')
    sb, cb, vb = parse(f'{SP}/snapB_{label}.txt')
    out = {}
    for s in STAGES:
        da, db = sa.get(s, {}), sb.get(s)
        if not db:
            continue
        dc = db['count'] - da.get('count', 0)
        ds = db['sum'] - da.get('sum', 0)
        if dc > 0:
            out[s] = (ds / dc * 1000, dc)
    wc = vb.get('stockflow_e2e_latency_websocket_ms_count', 0) - va.get('stockflow_e2e_latency_websocket_ms_count', 0)
    ws = vb.get('stockflow_e2e_latency_websocket_ms_sum', 0) - va.get('stockflow_e2e_latency_websocket_ms_sum', 0)
    out['_ws_avg'] = (ws / wc if wc else 0, wc)
    for k in ('p50', 'p90', 'p99'):
        out['_e2e_' + k] = (vb.get('stockflow_e2e_latency_' + k, 0), 0)
    out['_redis_cmds'] = (sum(cb.get(k, 0) for k in ('GET', 'SET', 'PUBLISH', 'EXISTS')), 0)
    out['_ws_threads'] = (vb.get('stockflow_ws_dispatch_threads', 0), 0)
    return out


def collect(prefix):
    runs = []
    for f in sorted(glob.glob(f'{SP}/snapB_{prefix}_r*.txt')):
        label = os.path.basename(f)[len('snapB_'):-len('.txt')]
        try:
            runs.append(run_metrics(label))
        except Exception as e:
            print(f'  (skip {label}: {e})', file=sys.stderr)
    return runs


def median_of(runs, key, idx=0):
    vals = [r[key][idx] for r in runs if key in r]
    return statistics.median(vals) if vals else None


conds = sys.argv[1:] or ['S0', 'SALL']
data = {c: collect(c) for c in conds}

for c in conds:
    print(f'{c}: {len(data[c])}회 측정')
print()

rows = STAGES + ['_ws_avg', '_e2e_p50', '_e2e_p90', '_e2e_p99', '_redis_cmds', '_ws_threads']
w = 30
print('지표'.ljust(w) + ''.join(c.rjust(14) for c in conds) + '     변화')
for key in rows:
    vals = [median_of(data[c], key) for c in conds]
    if all(v is None for v in vals):
        continue
    cells = ''.join(('-' if v is None else f'{v:,.3f}').rjust(14) for v in vals)
    delta = ''
    if vals[0] and vals[-1] is not None and len(conds) > 1:
        if vals[-1] == 0:
            delta = '  제거됨'
        else:
            delta = f'  {vals[0] / vals[-1]:.2f}x' if vals[-1] < vals[0] else f'  +{(vals[-1] / vals[0] - 1) * 100:.0f}%'
    elif vals[-1] == 0 and vals[0]:
        delta = '  제거됨'
    print(key.ljust(w) + cells + delta)

print()
for c in conds:
    n = [r['_ws_avg'][1] for r in data[c] if '_ws_avg' in r]
    print(f'{c} 본측정 처리건수: {n}')
