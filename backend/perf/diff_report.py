"""두 prometheus 스냅샷을 차분해 본측정 구간만의 평균을 낸다.

카운터/합계는 누적이므로 (B-A)/(countB-countA) 가 본측정 구간의 평균이 된다.
E2E 게이지(p50/p90/p99)는 최근 샘플 기준 롤링 값이라 B 시점 값을 그대로 쓴다.
"""
import re
import sys

A, B, LABEL = sys.argv[1], sys.argv[2], sys.argv[3]


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


sa, ca, va = parse(A)
sb, cb, vb = parse(B)

print(f'\n########## {LABEL} ##########')
print(f"{'stage':<32}{'건수':>9}{'mean ms':>10}")
order = ['realtime.total', 'redis.prev_close_get', 'redis.set_latest', 'redis.publish',
         'redis.set_publish_pipelined', 'snapshot.serialize', 'ws.dispatch',
         'storage.idempotency_check', 'storage.tx_total',
         'storage.db_insert', 'storage.instrument_registry', 'storage.idempotency_mark']
for s in order:
    da, db = sa.get(s, {'count': 0, 'sum': 0}), sb.get(s)
    if not db:
        continue
    dc = db['count'] - da.get('count', 0)
    ds = db['sum'] - da.get('sum', 0)
    if dc <= 0:
        continue
    print(f'{s:<32}{int(dc):>9}{ds / dc * 1000:>10.3f}')

wc = vb.get('stockflow_e2e_latency_websocket_ms_count', 0) - va.get('stockflow_e2e_latency_websocket_ms_count', 0)
ws = vb.get('stockflow_e2e_latency_websocket_ms_sum', 0) - va.get('stockflow_e2e_latency_websocket_ms_sum', 0)
print()
print(f"E2E @ Redis     : p50 {vb.get('stockflow_e2e_latency_p50'):.0f}ms  "
      f"p90 {vb.get('stockflow_e2e_latency_p90'):.0f}ms  p99 {vb.get('stockflow_e2e_latency_p99'):.0f}ms")
if wc:
    print(f"E2E @ WebSocket : avg {ws / wc:.2f}ms  (본측정 {int(wc)}건)")
# Lettuce 지연 메트릭은 발행 주기마다 리셋되므로 차분하지 않고 B 시점 누적값을 쓴다
print("Redis 명령(누적): " + '  '.join(f'{k}={cb.get(k, 0):.0f}' for k in ['GET', 'SET', 'PUBLISH', 'EXISTS']))
hc = vb.get('hikaricp_connections_acquire_seconds_count', 0) - va.get('hikaricp_connections_acquire_seconds_count', 0)
hs = vb.get('hikaricp_connections_acquire_seconds_sum', 0) - va.get('hikaricp_connections_acquire_seconds_sum', 0)
print(f"Hikari          : pending={vb.get('hikaricp_connections_pending'):.0f} "
      f"timeout={vb.get('hikaricp_connections_timeout_total'):.0f} "
      f"acquire_mean={hs / hc * 1000 if hc else 0:.3f}ms")
print(f"WS 스레드 누적  : {vb.get('stockflow_ws_dispatch_threads'):.0f}")
