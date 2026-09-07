#!/usr/bin/env bash
# 한 시점의 시스템 상태를 파일 묶음으로 남긴다. tps-sweep.sh 가 rate 구간 앞뒤로 호출한다.
# 독립적인 수집은 병렬로 돌리고 각 단계에 타임아웃을 건다 (kafka-consumer-groups 가 느릴 수 있음).
#
# 사용법: ./snapshot.sh <출력_프리픽스>
#   → <prefix>.prom  <prefix>.lag  <prefix>.redis  <prefix>.pg  <prefix>.stats  <prefix>.meta
set -uo pipefail

PREFIX=${1:?출력 프리픽스를 지정하세요}
APP_URL=${APP_URL:-http://localhost:8081}
APP_C=${APP_C:-stockflow-realtime}
KAFKA_C=${KAFKA_C:-stockflow-kafka}
REDIS_C=${REDIS_C:-stockflow-redis}
PG_C=${PG_C:-stockflow-timescaledb}
REALTIME_GROUP=${REALTIME_GROUP:-realtime-group}
STORAGE_GROUP=${STORAGE_GROUP:-storage-group}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}
LAG_VIA_KAFKA=${LAG_VIA_KAFKA:-0}   # 1이면 kafka-consumer-groups 로도 lag 수집 (느림)

mkdir -p "$(dirname "$PREFIX")"
tmo() { timeout "$@" 2>/dev/null; }

echo "# snapshot_epoch=$(date +%s) $(date -Is)" > "$PREFIX.meta"

# 1) 앱 지표 (가장 중요 — 동기)
curl -s --max-time 10 "$APP_URL/actuator/prometheus" > "$PREFIX.prom" 2>/dev/null || echo "(prom 실패)" > "$PREFIX.prom"

# 2~5) 나머지는 병렬
{
  if [ "$LAG_VIA_KAFKA" = "1" ]; then
    { echo "== $REALTIME_GROUP =="; tmo 60 docker exec "$KAFKA_C" kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP" --group "$REALTIME_GROUP" --describe
      echo; echo "== $STORAGE_GROUP =="; tmo 60 docker exec "$KAFKA_C" kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP" --group "$STORAGE_GROUP" --describe
    } > "$PREFIX.lag" 2>&1
  else
    # prom 의 records_lag 로 그룹별 합만 (빠름)
    awk '/^kafka_consumer_fetch_manager_records_lag\{/{
      if ($0 ~ /realtime-group/) rt+=$NF; else if ($0 ~ /storage-group/) st+=$NF
    } END{printf "realtime_lag_sum=%d\nstorage_lag_sum=%d\n", rt, st}' "$PREFIX.prom" > "$PREFIX.lag"
  fi
} &

tmo 15 docker exec "$REDIS_C" redis-cli INFO > "$PREFIX.redis" 2>/dev/null || echo "(redis 실패)" > "$PREFIX.redis" &

# count(*) 는 857M 행 seqscan 이라 금지. tup_inserted 델타로 삽입량을 잡고,
# 행수는 통계 추정치(n_live_tup)만 참고로 남긴다.
tmo 20 docker exec "$PG_C" psql -U postgres -d stockflow -qAt -F$'\t' \
  -c "select 'tup_inserted', tup_inserted, 'tup_updated', tup_updated, 'xact_commit', xact_commit, 'blks_read', blks_read, 'blks_hit', blks_hit from pg_stat_database where datname='stockflow'" \
  -c "select 'market_ticks_est_rows', n_live_tup from pg_stat_user_tables where relname='market_ticks'" \
  -c "select 'backends', count(*), 'active', count(*) filter (where state='active'), 'lockwait', count(*) filter (where wait_event_type='Lock') from pg_stat_activity where datname='stockflow'" \
  > "$PREFIX.pg" 2>&1 || echo "(pg 실패)" > "$PREFIX.pg" &

# 컨테이너 전체 stats 는 이 서버에서 ~20s 걸려서 핵심 4개만.
tmo 20 docker stats --no-stream --format \
  '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}' \
  "$APP_C" "$KAFKA_C" "$REDIS_C" "$PG_C" > "$PREFIX.stats" 2>/dev/null || echo "(stats 실패)" > "$PREFIX.stats" &

wait
echo "snapshot -> $PREFIX.{prom,lag,redis,pg,stats}"
