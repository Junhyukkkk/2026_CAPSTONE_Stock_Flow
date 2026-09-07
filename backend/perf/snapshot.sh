#!/usr/bin/env bash
# 한 시점의 시스템 상태를 파일 묶음으로 남긴다. tps-sweep.sh 가 rate 구간 앞뒤로 호출한다.
#
# 사용법: ./snapshot.sh <출력_프리픽스>
#   예) ./snapshot.sh results/sweep_p6/5000_A
#       → 5000_A.prom  5000_A.lag  5000_A.redis  5000_A.pg  5000_A.stats
#
# 환경변수:
#   APP_URL         (기본 http://localhost:8081)
#   KAFKA_C         kafka 컨테이너 (기본 stockflow-kafka)
#   REDIS_C         redis 컨테이너 (기본 stockflow-redis)
#   PG_C            timescaledb 컨테이너 (기본 stockflow-timescaledb)
#   REALTIME_GROUP  (기본 realtime-group)
#   STORAGE_GROUP   (기본 storage-group)
set -uo pipefail

PREFIX=${1:?출력 프리픽스를 지정하세요}
APP_URL=${APP_URL:-http://localhost:8081}
KAFKA_C=${KAFKA_C:-stockflow-kafka}
REDIS_C=${REDIS_C:-stockflow-redis}
PG_C=${PG_C:-stockflow-timescaledb}
REALTIME_GROUP=${REALTIME_GROUP:-realtime-group}
STORAGE_GROUP=${STORAGE_GROUP:-storage-group}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP:-localhost:9092}

mkdir -p "$(dirname "$PREFIX")"
ts=$(date +%s)
echo "# snapshot_epoch=$ts $(date -Is)" > "$PREFIX.meta"

# 1) 앱 지표 (Prometheus 노출)
curl -s --max-time 10 "$APP_URL/actuator/prometheus" > "$PREFIX.prom" 2>/dev/null || echo "(app prometheus 조회 실패)" > "$PREFIX.prom"

# 2) Consumer lag (두 그룹)
{
  echo "== $REALTIME_GROUP =="
  docker exec "$KAFKA_C" kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --group "$REALTIME_GROUP" --describe 2>/dev/null
  echo
  echo "== $STORAGE_GROUP =="
  docker exec "$KAFKA_C" kafka-consumer-groups --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --group "$STORAGE_GROUP" --describe 2>/dev/null
} > "$PREFIX.lag" 2>&1

# 3) Redis
docker exec "$REDIS_C" redis-cli INFO 2>/dev/null > "$PREFIX.redis" || echo "(redis 조회 실패)" > "$PREFIX.redis"

# 4) TimescaleDB — 저장량과 커밋/락 통계
docker exec "$PG_C" psql -U postgres -d stockflow -qAt -F$'\t' 2>/dev/null \
  -c "select 'market_ticks_rows', count(*) from market_ticks" \
  -c "select 'xact_commit', xact_commit, 'xact_rollback', xact_rollback, 'blks_read', blks_read, 'blks_hit', blks_hit, 'tup_inserted', tup_inserted from pg_stat_database where datname='stockflow'" \
  -c "select 'backends', count(*), 'active', count(*) filter (where state='active'), 'waiting', count(*) filter (where wait_event_type='Lock') from pg_stat_activity where datname='stockflow'" \
  > "$PREFIX.pg" 2>&1 || echo "(pg 조회 실패)" > "$PREFIX.pg"

# 5) 컨테이너 자원 (호스트 전체 용량 판단용)
docker stats --no-stream --format \
  '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}\t{{.NetIO}}\t{{.BlockIO}}' \
  2>/dev/null > "$PREFIX.stats" || echo "(docker stats 실패)" > "$PREFIX.stats"

echo "snapshot -> $PREFIX.{prom,lag,redis,pg,stats}"
