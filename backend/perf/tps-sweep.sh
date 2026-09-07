#!/usr/bin/env bash
# 서버 처리량(TPS) 스윕 — "이 서버는 초당 몇 건까지 소화하는가"를 재는 메인 스크립트
#
# 각 목표 rate 로 일정 시간 부하를 주고, 그동안 Consumer lag 이 평형을 이루는지
# (= 따라잡음) 계속 증가하는지(= 포화)를 관찰한다. 부하 종료 후 lag 이 배수되는
# 시간도 잰다. 마지막에 rate 별 판정과 병목 자원을 SUMMARY.md 로 정리한다.
#
# 스택이 이미 떠 있는 상태에서 돌린다 (docker compose / docker run 무관).
# loadgen 은 collectors 이미지를 재사용해 별도 컨테이너로 띄운다.
#
#   ./tps-sweep.sh p6                # 현재 파티션 그대로
#   ./tps-sweep.sh p12 12            # 토픽을 12 파티션으로 늘려 재측정
#
# 인자:  $1 = 라벨(결과 폴더명), $2 = (선택) 파티션 수 → 지정 시 토픽을 그 수로 ALTER
#
# 주요 환경변수 (기본값):
#   RATES="2000 3000 4000 4300 5000 6000 7000 8000"
#   HOLD=180  DRAIN_WAIT=150  SETTLE=30  SAMPLE_INTERVAL=5
#   WORKERS=6  SYMBOLS=50  STOP_COLLECTORS=1
#   NETWORK=infra_default  LOADGEN_IMAGE=collectors-binance-collector:latest
#   COLLECTORS_DIR=/home/capstone01/capstone/collectors
#   APP_C=stockflow-realtime  KAFKA_C=stockflow-kafka  REDIS_C=stockflow-redis  PG_C=stockflow-timescaledb
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL=${1:?"라벨을 지정하세요 (예: ./tps-sweep.sh p6)"}
ALTER_PARTITIONS=${2:-}

RATES=${RATES:-"2000 3000 4000 4300 5000 6000 7000 8000"}
HOLD=${HOLD:-180}
DRAIN_WAIT=${DRAIN_WAIT:-150}
SETTLE=${SETTLE:-30}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-5}
WORKERS=${WORKERS:-6}
SYMBOLS=${SYMBOLS:-50}
STOP_COLLECTORS=${STOP_COLLECTORS:-1}

NETWORK=${NETWORK:-infra_default}
LOADGEN_IMAGE=${LOADGEN_IMAGE:-collectors-binance-collector:latest}
COLLECTORS_DIR=${COLLECTORS_DIR:-/home/capstone01/capstone/collectors}
PERF_DIR="$SCRIPT_DIR"

APP_URL=${APP_URL:-http://localhost:8081}
APP_C=${APP_C:-stockflow-realtime}
KAFKA_C=${KAFKA_C:-stockflow-kafka}
REDIS_C=${REDIS_C:-stockflow-redis}
PG_C=${PG_C:-stockflow-timescaledb}
COLLECTORS=${COLLECTORS:-"stockflow-binance-collector stockflow-alpaca-collector"}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
TOPIC=${TOPIC:-market.normalized}
REALTIME_GROUP=${REALTIME_GROUP:-realtime-group}
STORAGE_GROUP=${STORAGE_GROUP:-storage-group}

OUT="$SCRIPT_DIR/results/sweep_${LABEL}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
export APP_URL KAFKA_C REDIS_C PG_C KAFKA_BOOTSTRAP REALTIME_GROUP STORAGE_GROUP

log() { echo -e "\033[1;36m[$(date +%H:%M:%S)]\033[0m $*"; }
app_prom() { curl -s --max-time 10 "$APP_URL/actuator/prometheus" 2>/dev/null; }
prom_sum() { awk '$0 !~ /^#/ { v=$NF; if (v+0==v) s+=v } END { printf "%.0f", s }'; }
total_lag() { app_prom | grep '^kafka_consumer_fetch_manager_records_lag{' | grep "$1-group" | prom_sum; }

run_loadgen() {  # $1=rate $2=duration $3=logfile
  docker run --rm --network "$NETWORK" \
    -v "$COLLECTORS_DIR":/app -v "$PERF_DIR":/perf -w /app \
    -e PYTHONPATH=/app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    -e RATE="$1" -e DURATION="$2" -e WORKERS="$WORKERS" -e SYMBOLS="$SYMBOLS" \
    -e BATCH_TICK_MS=5 -e TOPIC="$TOPIC" \
    "$LOADGEN_IMAGE" python /perf/loadgen.py > "$3" 2>&1
}

wait_health() {
  for _ in $(seq 1 60); do
    curl -s --max-time 3 "$APP_URL/actuator/health" 2>/dev/null | grep -q '"UP"' && { log "앱 UP"; return 0; }
    sleep 3
  done
  log "!! 앱이 UP 되지 않음"; exit 1
}

# ── 0. 환경 기록 ────────────────────────────────────────────────
{
  echo "# TPS 스윕 환경  $(date -Is)"
  echo "label=$LABEL rates=$RATES hold=${HOLD}s workers=$WORKERS"
  echo; echo "== host =="; echo "nproc=$(nproc)"; free -h; uname -a; uptime
  echo; echo "== kafka topic =="
  docker exec "$KAFKA_C" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --describe --topic "$TOPIC" 2>/dev/null
  echo; echo "== redis =="
  docker exec "$REDIS_C" redis-cli CONFIG GET maxmemory 2>/dev/null | tr '\n' ' '; echo
  docker exec "$REDIS_C" redis-cli CONFIG GET maxmemory-policy 2>/dev/null | tr '\n' ' '; echo
  echo; echo "== app image / env =="
  docker inspect "$APP_C" --format 'image={{.Config.Image}} started={{.State.StartedAt}}' 2>/dev/null
  docker exec "$APP_C" env 2>/dev/null | grep -iE "KAFKA_CONSUMER|STORAGE_CONSUMER|STOCKFLOW_OPT|SPRING_PROFILES|JAVA_TOOL" | sort
  echo; echo "== code rev =="
  (cd "$SCRIPT_DIR/.." && git log -1 --oneline 2>/dev/null)
} > "$OUT/env.txt" 2>&1
log "환경 → $OUT/env.txt"
wait_health

# ── 1. 파티션 조정 (옵션) ───────────────────────────────────────
CUR_PARTS=$(docker exec "$KAFKA_C" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --describe --topic "$TOPIC" 2>/dev/null | grep -oE 'PartitionCount: *[0-9]+' | grep -oE '[0-9]+' | head -1)
log "$TOPIC 파티션 수: ${CUR_PARTS:-?}"
if [ -n "$ALTER_PARTITIONS" ] && [ "$ALTER_PARTITIONS" != "${CUR_PARTS:-}" ]; then
  log "파티션 $CUR_PARTS → $ALTER_PARTITIONS ALTER (되돌릴 수 없음 — 부하 테스트라 무방)"
  docker exec "$KAFKA_C" kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --alter --topic "$TOPIC" --partitions "$ALTER_PARTITIONS"
  log "앱 재기동(Consumer 재조정)"; docker restart "$APP_C" >/dev/null; wait_health; sleep 20
fi

# ── 2. 실데이터 수집기 정지 ────────────────────────────────────
STOPPED=""
if [ "$STOP_COLLECTORS" = "1" ]; then
  for c in $COLLECTORS; do
    docker stop "$c" >/dev/null 2>&1 && STOPPED="$STOPPED $c" && log "정지: $c" || true
  done
fi
cleanup() {
  set +e
  [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" 2>/dev/null
  for c in $STOPPED; do docker start "$c" >/dev/null 2>&1 && log "재기동: $c"; done
}
trap cleanup EXIT

# ── 3. 표본 수집기 (백그라운드, SAMPLE_INTERVAL 초) ────────────
TIMELINE="$OUT/timeline.csv"
sampler() {
  echo "epoch,phase,rate,lag_realtime,lag_storage,consumed_realtime,consumed_storage,throughput,proc_time_avg,redis_mem_mb,redis_evicted,redis_ops,mem_free_mb,swap_used_mb,load1,cpu_app,cpu_kafka,cpu_redis,cpu_pg" > "$TIMELINE"
  while true; do
    local prom redis stats free_out
    prom=$(app_prom)
    redis=$(docker exec "$REDIS_C" redis-cli INFO 2>/dev/null | tr -d '\r')
    stats=$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' 2>/dev/null)
    free_out=$(free -m 2>/dev/null)
    g() { echo "$prom" | grep "^kafka_consumer_fetch_manager_records_$1{" | grep "$2-group" | prom_sum; }
    rf() { echo "$redis" | awk -F: -v k="$1" '$1==k{print $2+0}'; }
    cpu() { echo "$stats" | awk -v n="$1" '$1==n{gsub(/%/,"",$2); print $2+0}'; }
    echo "$(date +%s),${PHASE:-idle},${CUR_RATE:-0},$(g lag realtime),$(g lag storage),$(g consumed_total realtime),$(g consumed_total storage),$(echo "$prom"|awk '/^stockflow_throughput_per_second /{print $2}'),$(echo "$prom"|awk '/^stockflow_processing_time_avg /{print $2}'),$(rf used_memory | awk '{printf "%.1f",$1/1048576}'),$(rf evicted_keys),$(rf instantaneous_ops_per_sec),$(echo "$free_out"|awk '/^Mem:/{print $4}'),$(echo "$free_out"|awk '/^Swap:/{print $3}'),$(awk '{print $1}' /proc/loadavg),$(cpu $APP_C),$(cpu $KAFKA_C),$(cpu $REDIS_C),$(cpu $PG_C)" >> "$TIMELINE"
    sleep "$SAMPLE_INTERVAL"
  done
}
PHASE=idle CUR_RATE=0 sampler & SAMPLER_PID=$!

# ── 4. rate 스윕 ───────────────────────────────────────────────
echo "rate,effective_send,consume_realtime,consume_storage,peak_lag_rt,end_lag_rt,drain_s,verdict" > "$OUT/summary.csv"
for RATE in $RATES; do
  export CUR_RATE=$RATE
  log "════════ rate=$RATE msg/s (${HOLD}s) ════════"
  export PHASE=snapA
  "$SCRIPT_DIR/snapshot.sh" "$OUT/${RATE}_A" >/dev/null
  a_epoch=$(date +%s)
  a_rt=$(grep '^kafka_consumer_fetch_manager_records_consumed_total{' "$OUT/${RATE}_A.prom" | grep realtime-group | prom_sum)
  a_st=$(grep '^kafka_consumer_fetch_manager_records_consumed_total{' "$OUT/${RATE}_A.prom" | grep storage-group | prom_sum)

  export PHASE=hold
  run_loadgen "$RATE" "$HOLD" "$OUT/${RATE}_loadgen.log" || log "!! loadgen 비정상 종료"
  b_epoch=$(date +%s)
  eff=$(grep -oE 'effective_rate=[0-9.]+' "$OUT/${RATE}_loadgen.log" | tail -1 | cut -d= -f2)
  log "loadgen 완료. 실효 ≈ ${eff:-?} msg/s"

  export PHASE=drain
  "$SCRIPT_DIR/snapshot.sh" "$OUT/${RATE}_B" >/dev/null
  b_rt=$(grep '^kafka_consumer_fetch_manager_records_consumed_total{' "$OUT/${RATE}_B.prom" | grep realtime-group | prom_sum)
  b_st=$(grep '^kafka_consumer_fetch_manager_records_consumed_total{' "$OUT/${RATE}_B.prom" | grep storage-group | prom_sum)
  dt=$((b_epoch - a_epoch)); [ "$dt" -lt 1 ] && dt=1
  crt=$(awk "BEGIN{printf \"%.0f\",($b_rt-$a_rt)/$dt}")
  cst=$(awk "BEGIN{printf \"%.0f\",($b_st-$a_st)/$dt}")

  log "lag 배수 대기 (상한 ${DRAIN_WAIT}s)"
  ds=$(date +%s); drained=timeout
  while [ $(( $(date +%s) - ds )) -lt "$DRAIN_WAIT" ]; do
    l=$(total_lag realtime); l=${l:-1}
    [ "$l" -le 300 ] 2>/dev/null && { drained=$(( $(date +%s) - ds )); break; }
    sleep 5
  done
  peak=$(awk -F, -v s="$a_epoch" -v e="$b_epoch" 'NR>1&&$1>=s&&$1<=e{if($4>m)m=$4}END{print m+0}' "$TIMELINE")
  endl=$(total_lag realtime)
  verdict=$(awk -v c="$crt" -v s="${eff:-0}" -v el="${endl:-0}" -v dr="$drained" 'BEGIN{
    if(s>0 && c/s>=0.92 && dr!="timeout") print "KEPT_UP";
    else if(dr=="timeout" || el>5000) print "SATURATED"; else print "MARGINAL"}')
  log "판정: $verdict  (소비 rt≈${crt}/s st≈${cst}/s, peak lag ${peak}, 배수 ${drained})"
  echo "$RATE,${eff:-0},$crt,$cst,$peak,$endl,$drained,$verdict" >> "$OUT/summary.csv"
  export PHASE=settle CUR_RATE=0; sleep "$SETTLE"
done

# ── 5. 리포트 ─────────────────────────────────────────────────
kill "$SAMPLER_PID" 2>/dev/null || true; SAMPLER_PID=
python3 "$SCRIPT_DIR/sweep_report.py" "$OUT" > "$OUT/SUMMARY.md" 2>&1 || cp "$OUT/summary.csv" "$OUT/SUMMARY.md"
log "완료 → $OUT/SUMMARY.md"
echo; cat "$OUT/SUMMARY.md"
