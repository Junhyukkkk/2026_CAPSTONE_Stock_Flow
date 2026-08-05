#!/usr/bin/env bash
# 조건별 성능 측정 러너
#
# 조건마다 동일하게 수행한다:
#   1) market_ticks / instruments 비우고 pg_stat 리셋, Redis FLUSHALL
#      (DB 성장과 누적 통계가 조건 간 비교를 오염시키지 않도록)
#   2) 앱을 지정한 env 로 새로 기동
#   3) 워밍업 -> 스냅샷A -> 본측정 -> 스냅샷B
#      차분으로 JIT 워밍업 구간을 측정에서 제외한다
#
# 사용법:
#   ./bench.sh <LABEL> [APP_ENV=val ...]
#
# 예:
#   ./bench.sh baseline STOCKFLOW_OPT_INSTRUMENT_REGISTRY_FIX=false
#   ./bench.sh tuned STOCKFLOW_OPT_INSTRUMENT_CACHE=true STOCKFLOW_OPT_WS_TASK_EXECUTOR=true
#
# 환경변수:
#   RATE(500) MEASURE_DURATION(120) WARMUP_DURATION(20) LINGER_MS(10) SYMBOLS(50)
#
# 주의: zsh 에서 여러 토글을 변수에 담아 넘길 때는 워드 스플리팅이 일어나지 않는다.
#       ${=VAR} 를 쓰거나 인자를 하나씩 직접 나열할 것.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COLLECTORS="$REPO_ROOT/collectors"
RESULTS="$SCRIPT_DIR/results"

IMAGE=${IMAGE:-infra-stockflow-realtime}
NETWORK=${NETWORK:-infra_default}
APP=stockflow-bench
LOADGEN_IMAGE=${LOADGEN_IMAGE:-stockflow-loadgen}

RATE=${RATE:-500}
WARMUP_DURATION=${WARMUP_DURATION:-20}
MEASURE_DURATION=${MEASURE_DURATION:-120}
LINGER_MS=${LINGER_MS:-10}
SYMBOLS=${SYMBOLS:-50}

if [ $# -lt 1 ]; then
  echo "usage: $0 <LABEL> [APP_ENV=val ...]" >&2
  exit 2
fi
LABEL=$1; shift

APPENV=()
for kv in "$@"; do APPENV+=(-e "$kv"); done

mkdir -p "$RESULTS"
cleanup() { docker rm -f "$APP" >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "########## $LABEL ##########"
echo "  app env: $*   linger.ms=$LINGER_MS  rate=$RATE"

# --- 1) 상태 초기화 ---
docker rm -f "$APP" >/dev/null 2>&1 || true
docker stop stockflow-realtime >/dev/null 2>&1 || true
docker exec stockflow-timescaledb psql -U postgres -d stockflow -q \
  -c "truncate table market_ticks;" \
  -c "truncate table instruments cascade;" \
  -c "select pg_stat_reset();" >/dev/null 2>&1
docker exec stockflow-redis redis-cli FLUSHALL >/dev/null 2>&1

# --- 2) 앱 기동 ---
docker run -d --name "$APP" --network "$NETWORK" -p 8081:8081 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_CONSUMER_GROUP_REALTIME=realtime-group \
  -e KAFKA_CONSUMER_GROUP_STORAGE=storage-group \
  -e KAFKA_CONSUMER_CONCURRENCY=12 \
  -e KAFKA_CONSUMER_MAX_POLL_RECORDS=100 \
  -e REDIS_HOST=redis -e REDIS_PORT=6379 \
  -e DB_HOST=timescaledb -e DB_PORT=5432 -e DB_NAME=stockflow \
  -e DB_USERNAME=postgres -e DB_PASSWORD=postgres \
  -e SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE=16 \
  -e JAVA_TOOL_OPTIONS="-Xmx1200m -Xms1200m" \
  "${APPENV[@]}" "$IMAGE" >/dev/null

until curl -s --max-time 3 http://localhost:8081/actuator/health 2>/dev/null | grep -q '"UP"'; do
  if ! docker ps -q --filter name="$APP" | grep -q .; then
    echo "!! 앱 기동 실패" >&2
    docker logs "$APP" 2>&1 | tail -30 >&2
    exit 1
  fi
  sleep 3
done

run_load() {
  docker run --rm --network "$NETWORK" \
    -v "$COLLECTORS":/app -v "$SCRIPT_DIR":/loadgen \
    -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 -e PYTHONPATH=/app \
    -e KAFKA_LINGER_MS="$LINGER_MS" \
    -e RATE="$RATE" -e DURATION="$1" -e SYMBOLS="$SYMBOLS" \
    -w /app "$LOADGEN_IMAGE" python /loadgen/loadgen.py 2>&1 | tail -1
}

# --- 3) 측정 ---
echo "-- 워밍업 ${WARMUP_DURATION}s --"
run_load "$WARMUP_DURATION" >/dev/null
sleep 3
curl -s --max-time 10 http://localhost:8081/actuator/prometheus > "$RESULTS/snapA_$LABEL.txt"

echo "-- 본측정 ${MEASURE_DURATION}s --"
run_load "$MEASURE_DURATION"
sleep 5
curl -s --max-time 10 http://localhost:8081/actuator/prometheus > "$RESULTS/snapB_$LABEL.txt"

docker exec stockflow-timescaledb psql -U postgres -d stockflow -t -c \
  "select 'instruments UPDATE 누적: '||n_tup_upd||', autovacuum: '||autovacuum_count
     from pg_stat_user_tables where relname='instruments';" 2>/dev/null | head -2

python3 "$SCRIPT_DIR/diff_report.py" "$RESULTS/snapA_$LABEL.txt" "$RESULTS/snapB_$LABEL.txt" "$LABEL"
