"""tps-sweep.sh 결과 폴더 → SUMMARY.md

각 rate 구간에 대해:
  - 실효 전송률 vs 소비율(realtime / storage)
  - peak lag / 배수 시간 / 판정
  - 그 구간에서 한계에 닿은 자원(컨테이너 CPU, Redis 축출, Hikari 대기)
마지막에 "이 서버 지속 가능 TPS ≈ X, 병목 = Y" 결론.
"""
import csv
import glob
import os
import re
import sys

OUT = sys.argv[1]


def read(path):
    try:
        return open(path, encoding='utf-8', errors='replace').read()
    except OSError:
        return ''


def prom_val(text, name):
    # 라벨 유무 모두 처리: `name 1.0` / `name{a="b"} 1.0`
    m = re.search(rf'^{re.escape(name)}(?:\{{[^}}]*\}})?\s+([-\d.eE+]+)', text, re.M)
    return float(m.group(1)) if m else None


def prom_sum(text, pattern):
    s = 0.0
    for line in text.splitlines():
        if line.startswith('#') or not re.search(pattern, line):
            continue
        try:
            s += float(line.rsplit(' ', 1)[1])
        except (ValueError, IndexError):
            pass
    return s


def redis_field(text, key):
    m = re.search(rf'^{re.escape(key)}:(.+)$', text, re.M)
    return m.group(1).strip() if m else None


def pg_field(text, key):
    m = re.search(rf'{re.escape(key)}\t([0-9.]+)', text)
    return float(m.group(1)) if m else None


env = read(f'{OUT}/env.txt')
nproc = re.search(r'nproc=(\d+)', env)
mem = re.search(r'Mem:\s+(\S+)', env)
parts = re.search(r'PartitionCount:\s*(\d+)', env)
gitrev = re.search(r'== code rev ==\n(\w+)', env)
stage_present = 'stockflow_stage_seconds' in read(glob.glob(f'{OUT}/*_B.prom')[0]) if glob.glob(f'{OUT}/*_B.prom') else False

rows = list(csv.DictReader(open(f'{OUT}/summary.csv')))

# timeline 을 rate 별 hold 구간으로 나눠 자원 피크 계산
tl = list(csv.DictReader(open(f'{OUT}/timeline.csv'))) if os.path.exists(f'{OUT}/timeline.csv') else []


def hold_window(rate):
    xs = [r for r in tl if r['rate'] == str(rate) and r['phase'] == 'hold']
    return xs


def peak(xs, col):
    vals = [float(x[col]) for x in xs if x.get(col) not in (None, '', 'nan')]
    return max(vals) if vals else 0.0


print(f'# 서버 처리량(TPS) 스윕 결과 — `{os.path.basename(OUT)}`\n')
print('## 측정 환경\n')
print(f'| 항목 | 값 |')
print(f'| --- | --- |')
print(f'| CPU 코어 | {nproc.group(1) if nproc else "?"} |')
print(f'| 메모리 | {mem.group(1) if mem else "?"} |')
print(f'| `{os.getenv("TOPIC", "market.normalized")}` 파티션 | {parts.group(1) if parts else "?"} |')
print(f'| 코드 리비전 | {gitrev.group(1) if gitrev else "?"} |')
print(f'| 구간 계측(stage_seconds) | {"있음" if stage_present else "**없음 (구버전 — git pull 후 재빌드 필요)**"} |')
print()

def numf(s):
    # tps-sweep.sh 가 브로커 CLI 무응답으로 측정을 못 하면 "NA" 문자열을 쓴다
    # (0 으로 착각해 델타를 내면 음수로 튀는 문제가 있었음, 9/21 v2). float() 로
    # 그대로 캐스팅하면 크래시하므로 여기서 NA 는 NA 로 통과시킨다.
    try:
        return f'{float(s):,.0f}'
    except ValueError:
        return s

print('## rate 스윕\n')
print('| 목표 rate | 실효 전송 | 소비(realtime) | 소비(storage) | peak lag | 배수 | 판정 |')
print('| ---: | ---: | ---: | ---: | ---: | ---: | :--- |')
for r in rows:
    dr = r['drain_s']
    dr = f'{dr}s' if dr.isdigit() else dr
    print(f"| {r['rate']} | {numf(r['effective_send'])} | {numf(r['consume_realtime'])} | "
          f"{numf(r['consume_storage'])} | {numf(r['peak_lag_rt'])} | {dr} | {r['verdict']} |")
print()

def lo(xs, col):
    vals = [float(x[col]) for x in xs if x.get(col) not in (None, '', 'nan')]
    return min(vals) if vals else 0.0


def redis_evicted(text):
    # snapshot.sh 는 redis-cli INFO 가 타임아웃/실패하면 파일에 "(redis 실패)" 라고만 쓴다.
    # redis_field 가 못 찾으면 None 인데, 예전엔 `or 0` 으로 0 취급해서 A는 성공(수천만)·
    # B는 실패(0 취급)인 경우 델타가 수천만 음수로 튀었다(9/21 v2, -6100만 등). None 은
    # None 으로 남겨 호출부가 "모름"으로 취급하게 한다.
    if '실패' in text:
        return None
    v = redis_field(text, 'evicted_keys')
    return float(v) if v is not None else None


print('## 구간별 자원 상태 (부하 중 피크)\n')
print('| rate | CPU app | CPU kafka | CPU redis | CPU pg | load1 | RAM free 최저 | swap | Redis 축출Δ | Hikari pending |')
print('| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |')
for r in rows:
    rate = r['rate']
    xs = hold_window(rate)
    ev_a = redis_evicted(read(f'{OUT}/{rate}_A.redis'))
    ev_b = redis_evicted(read(f'{OUT}/{rate}_B.redis'))
    ev_delta = f'{ev_b - ev_a:,.0f}' if ev_a is not None and ev_b is not None else 'NA'
    hp = prom_val(read(f'{OUT}/{rate}_B.prom'), 'hikaricp_connections_pending') or 0
    cpu_cols = [peak(xs, c) for c in ('cpu_app', 'cpu_kafka', 'cpu_redis', 'cpu_pg')]
    # 자원 샘플러가 그 구간 내내(모든 표본) 실패해 4개 CPU 열이 동시에 0 이면 실제 0%가
    # 아니라 측정 자체가 안 된 것이다(호스트 과부하로 docker stats 까지 응답 못한 경우,
    # 9/21 v2 rate>=8000 구간에서 관측). 진짜 유휴 CPU와 구분해 NA로 표시한다.
    cpu_txt = ['NA'] * 4 if not xs or all(v == 0 for v in cpu_cols) else [f'{v:.0f}%' for v in cpu_cols]
    print(f"| {rate} | {cpu_txt[0]} | {cpu_txt[1]} | {cpu_txt[2]} | {cpu_txt[3]} | "
          f"{peak(xs,'load1'):.1f} | {lo(xs,'mem_free_mb'):.0f}MB | "
          f"{peak(xs,'swap_used_mb'):.0f}MB | {ev_delta} | {hp:.0f} |")
print()

kept = [r for r in rows if r['verdict'] == 'KEPT_UP']
sat = [r for r in rows if r['verdict'] == 'SATURATED']
ceiling = max((int(r['rate']) for r in kept), default=None)
first_sat = min((int(r['rate']) for r in sat), default=None)

print('## 결론\n')
if ceiling:
    print(f'- **이 서버가 지속적으로 소화한 최대 rate ≈ {ceiling:,} msg/s** '
          f'(이 rate까지는 부하 중 realtime 소비율이 전송률을 따라갔고 적체가 얕게 유지됨)')
else:
    print('- **가장 낮은 테스트 rate 부터 이미 포화** — 지속 처리량은 그보다 낮음. '
          'RATES 를 더 낮춰 다시 측정 필요.')
if first_sat:
    sr = next(r for r in rows if r['rate'] == str(first_sat))
    xs = hold_window(first_sat)
    # 병목 추정
    cand = []
    if peak(xs, 'cpu_app') > 85:
        cand.append(f"앱(JVM) CPU 포화 ({peak(xs,'cpu_app'):.0f}%)")
    if peak(xs, 'cpu_redis') > 85:
        cand.append(f"Redis CPU 포화 ({peak(xs,'cpu_redis'):.0f}%)")
    if peak(xs, 'cpu_pg') > 85:
        cand.append(f"TimescaleDB CPU 포화 ({peak(xs,'cpu_pg'):.0f}%)")
    ev_a0 = redis_evicted(read(f'{OUT}/{first_sat}_A.redis'))
    ev_b0 = redis_evicted(read(f'{OUT}/{first_sat}_B.redis'))
    if ev_a0 is not None and ev_b0 is not None and ev_b0 - ev_a0 > 1000:
        cand.append('Redis maxmemory 도달 → 키 축출 (멱등성 키 누적)')
    if (prom_val(read(f'{OUT}/{first_sat}_B.prom'), 'hikaricp_connections_pending') or 0) > 0:
        cand.append('Hikari 커넥션 풀 대기 (DB 저장 경로)')
    total_cpu = peak(xs, 'cpu_app') + peak(xs, 'cpu_kafka') + peak(xs, 'cpu_redis') + peak(xs, 'cpu_pg')
    if not cand and nproc and total_cpu > int(nproc.group(1)) * 80:
        cand.append(f'호스트 CPU 총량 포화 (컨테이너 합 {total_cpu:.0f}% / {nproc.group(1)}코어)')
    print(f'- **처음 포화된 rate = {first_sat:,} msg/s.** 그 구간 병목 후보: '
          f'{", ".join(cand) if cand else "자원 피크 뚜렷하지 않음 — timeline.csv 수동 확인 필요"}')
print(f'- 상세: `{OUT}/timeline.csv`, `*_A/_B.{{prom,lag,redis,pg,stats}}`, `*_loadgen.log`')
