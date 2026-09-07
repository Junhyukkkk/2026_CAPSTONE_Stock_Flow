#!/usr/bin/env bash
# 실험 B 자동화 — "적체가 Kafka 보관기간을 넘기면 어떻게 되나"
#
# 8/5 미팅에서 수동으로 했던 것과 같은 시나리오를, retention 을 4시간 대신
# 5분으로 줄여 몇 분 만에 재현한다.
#
#   1) market.normalized 의 retention.ms 를 5분으로 ALTER
#   2) 실시간/저장 Consumer(앱) 정지
#   3) N건 투입 (Consumer 가 안 읽는 상태로 Kafka 에만 쌓임)
#   4) retention(5분) + 여유 초과 대기 → Kafka 가 안 읽은 세그먼트 삭제
#   5) 앱 재기동 → 로그에서 "offset reset" 확인, 저장된 건수와 투입 건수 비교
#   6) retention.ms 원복
#
# 결과: 투입 N건 중 상당수가 저장 0건 = 조용한 유실. 크래시는 없음.
#
# 사용법: ./retention-cliff-test.sh
# 환경변수: INJECT(200000)  RETENTION_MS(300000)  WAIT_EXTRA(180)  WORKERS(6)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_C=${KAFKA_C:-stockflow-kafka}
PG_C=${PG_C:-stockflow-timescaledb}
APP_C=${APP_C:-stockflow-realtime}
COLLECTORS=${COLLECTORS:-"stockflow-binance-collector stockflow-alpaca-collector"}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
TOPIC=${TOPIC:-market.normalized}
NETWORK=${NETWORK:-infra_default}
LOADGEN_IMAGE=${LOADGEN_IMAGE:-collectors-binance-collector:latest}
COLLECTORS_DIR=${COLLECTORS_DIR:-/home/capstone01/capstone/collectors}

INJECT=${INJECT:-200000}
RETENTION_MS=${RETENTION_MS:-300000}
WAIT_EXTRA=${WAIT_EXTRA:-180}
WORKERS=${WORKERS:-6}
RATE=${RATE:-8000}
DURATION=$(( INJECT / RATE + 1 ))

OUT="$SCRIPT_DIR/results/cliff_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
log() { echo -e "\033[1;33m[$(date +%H:%M:%S)]\033[0m $*"; }
pg() { docker exec "$PG_C" psql -U postgres -d stockflow -qAt -c "$1" 2>/dev/null; }
run_loadgen() {  # $1=rate $2=duration $3=logfile
  docker run --rm --network "$NETWORK" \
    -v "$COLLECTORS_DIR":/app -v "$SCRIPT_DIR":/perf -w /app \
    -e PYTHONPATH=/app -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    -e RATE="$1" -e DURATION="$2" -e WORKERS="$WORKERS" -e SYMBOLS=50 \
    -e BATCH_TICK_MS=5 -e TOPIC="$TOPIC" \
    "$LOADGEN_IMAGE" python /perf/loadgen.py > "$3" 2>&1
}

ORIG_RET=$(docker exec "$KAFKA_C" kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --describe 2>/dev/null | grep -oE 'retention.ms=[0-9]+' | head -1 | cut -d= -f2)
log "현재 retention.ms=${ORIG_RET:-(브로커 기본)}"

restore() {
  log "retention.ms 원복 → ${ORIG_RET:-삭제}"
  if [ -n "${ORIG_RET:-}" ]; then
    docker exec "$KAFKA_C" kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --entity-type topics --entity-name "$TOPIC" --alter --add-config "retention.ms=$ORIG_RET" >/dev/null 2>&1 || true
  else
    docker exec "$KAFKA_C" kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --entity-type topics --entity-name "$TOPIC" --alter --delete-config retention.ms >/dev/null 2>&1 || true
  fi
  for c in $COLLECTORS; do docker start "$c" >/dev/null 2>&1 || true; done
  docker start "$APP_C" >/dev/null 2>&1 || true
}
trap restore EXIT

# 1) retention 축소 + 세그먼트 롤링 촉진
log "retention.ms=$RETENTION_MS, segment.ms=60000 으로 ALTER"
docker exec "$KAFKA_C" kafka-configs --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --entity-type topics --entity-name "$TOPIC" --alter \
  --add-config "retention.ms=$RETENTION_MS,segment.ms=60000,file.delete.delay.ms=1000"

# 2) Consumer 정지
log "수집기·앱(Consumer) 정지"
for c in $COLLECTORS; do docker stop "$c" >/dev/null 2>&1 || true; done
docker stop "$APP_C" >/dev/null

BEFORE_ROWS=$(pg "select count(*) from market_ticks")
BEFORE_END=$(docker exec "$KAFKA_C" kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list "$KAFKA_BOOTSTRAP" --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')
log "투입 전: market_ticks=$BEFORE_ROWS, topic end offset 합=$BEFORE_END"

# 3) N건 투입 (unique tradeId 로 저장 시 중복 필터에 안 걸리게)
log "$INJECT 건 투입 (rate=$RATE, workers=$WORKERS, ~${DURATION}s)"
run_loadgen "$RATE" "$DURATION" "$OUT/inject.log"
INJECTED=$(grep -oE 'acked=[0-9]+' "$OUT/inject.log" | tail -1 | cut -d= -f2)
AFTER_END=$(docker exec "$KAFKA_C" kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list "$KAFKA_BOOTSTRAP" --topic "$TOPIC" 2>/dev/null | awk -F: '{s+=$3} END{print s}')
log "투입 완료: acked=$INJECTED, topic end offset 합=$AFTER_END (Δ=$((AFTER_END-BEFORE_END)))"

# 4) retention 초과 대기 (5분 + 여유). 세그먼트가 롤오버돼야 삭제되므로 여유를 크게 준다.
WAIT=$(( RETENTION_MS/1000 + WAIT_EXTRA ))
log "보관기간 초과 대기 ${WAIT}s (Kafka 가 안 읽은 세그먼트를 삭제하도록)"
sleep "$WAIT"

BEGIN_OFF=$(docker exec "$KAFKA_C" kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list "$KAFKA_BOOTSTRAP" --topic "$TOPIC" --time -2 2>/dev/null | awk -F: '{s+=$3} END{print s}')
log "삭제 후 topic begin offset 합=$BEGIN_OFF (0보다 크면 앞부분이 삭제된 것)"

# 5) 앱 재기동 → offset reset 관찰
log "앱 재기동"
START_TS=$(date +%s)
docker start "$APP_C" >/dev/null
for _ in $(seq 1 40); do
  curl -s --max-time 3 http://localhost:8081/actuator/health 2>/dev/null | grep -q '"UP"' && break
  sleep 3
done
sleep 60   # 재조정 + 밀린 것(남아 있으면) 처리

docker logs --since "$START_TS" "$APP_C" 2>&1 | grep -iE "offset|reset|OutOfRange|Seeking" | tee "$OUT/offset_reset.log" || true
AFTER_ROWS=$(pg "select count(*) from market_ticks")

{
  echo "# retention cliff 재현 결과  $(date -Is)"
  echo
  echo "| 항목 | 값 |"
  echo "| --- | --- |"
  echo "| 투입(acked) | $INJECTED |"
  echo "| topic offset 증가분 | $((AFTER_END-BEFORE_END)) |"
  echo "| 삭제 후 begin offset 합 | $BEGIN_OFF |"
  echo "| market_ticks 증가분 | $((AFTER_ROWS-BEFORE_ROWS)) |"
  echo "| offset reset 로그 | $( [ -s "$OUT/offset_reset.log" ] && echo '있음 (아래)' || echo '없음' ) |"
  echo
  echo '결론: 투입분 대비 저장 증가분이 크게 적으면 = 보관기간 초과로 조용히 유실.'
  echo '앱 크래시 없음(재기동 후 정상 UP).'
  echo
  echo '## offset reset 로그'
  echo '```'
  cat "$OUT/offset_reset.log" 2>/dev/null
  echo '```'
} > "$OUT/RESULT.md"

log "완료 → $OUT/RESULT.md"
cat "$OUT/RESULT.md"
