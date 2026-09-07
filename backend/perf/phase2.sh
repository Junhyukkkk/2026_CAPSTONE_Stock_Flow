#!/usr/bin/env bash
# Phase 2: 최신 main + 12파티션으로 stockflow-realtime 교체 (되돌릴 수 있음)
#   ./phase2.sh build    → worktree + 이미지 빌드만
#   ./phase2.sh deploy    → 빌드된 이미지로 컨테이너 교체 + 토픽 12파티션
#   ./phase2.sh restore  → 원래 컨테이너로 복구
#   ./phase2.sh status
set -euo pipefail
REPO=/home/capstone01/capstone
WT=/home/capstone01/capstone-main
IMG=stockflow-realtime:main
ACTION=${1:-status}
PAUSE_DURING_BUILD="stockflow-analysis stockflow-alpaca-collector stockflow-kafka-ui stockflow-redis-insight"

case "$ACTION" in
build)
  cd "$REPO"
  git fetch origin main
  if [ -d "$WT/.git" ] || [ -f "$WT/.git" ]; then (cd "$WT" && git fetch origin main && git reset --hard origin/main); else git worktree add -f "$WT" origin/main; fi
  echo ">> main rev: $(cd "$WT" && git rev-parse --short HEAD)  ($(cd "$WT" && git log -1 --format=%s))"
  echo ">> 빌드 중 리소스 확보: $PAUSE_DURING_BUILD 정지"
  for c in $PAUSE_DURING_BUILD; do docker stop "$c" >/dev/null 2>&1 || true; done
  echo ">> 이미지 빌드 (gradle 이미지 pull 포함, 수 분)"
  set +e
  DOCKER_BUILDKIT=1 docker build -t "$IMG" -f "$WT/backend/Dockerfile" "$WT/backend"
  rc=$?
  set -e
  for c in $PAUSE_DURING_BUILD; do docker start "$c" >/dev/null 2>&1 || true; done
  [ $rc -eq 0 ] && echo ">> 빌드 성공: $IMG" || { echo "!! 빌드 실패 (rc=$rc)"; exit $rc; }
  ;;

deploy)
  exec > >(tee /tmp/p2deploy.out) 2>&1
  set -x
  docker image inspect "$IMG" >/dev/null 2>&1 || { echo "!! $IMG 없음 — 먼저 ./phase2.sh build"; exit 1; }
  docker exec stockflow-kafka kafka-topics --bootstrap-server localhost:9092 \
    --alter --topic market.normalized --partitions 12 || echo "(이미 12거나 실패)"
  docker inspect stockflow-realtime --format '{{range .Config.Env}}{{println .}}{{end}}' \
    | grep -E '^(KAFKA_|REDIS_|DB_|SPRING_|RETRY_|LOKI_|SENTRY_|STOCKFLOW_|JAVA_TOOL_OPTIONS|MONITORING_)' > /tmp/rt.env
  wc -l /tmp/rt.env
  docker stop stockflow-realtime
  docker rename stockflow-realtime stockflow-realtime-p1
  docker run -d --name stockflow-realtime --network infra_default -p 8081:8081 \
    --restart unless-stopped --env-file /tmp/rt.env "$IMG"
  set +x
  for i in $(seq 1 60); do
    curl -s --max-time 3 localhost:8081/actuator/health 2>/dev/null | grep -q '"UP"' && { echo "UP after $((i*3))s"; break; }
    sleep 3
  done
  sleep 15
  docker exec stockflow-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic market.normalized 2>/dev/null | head -1
  echo "new metric count: $(curl -s localhost:8081/actuator/prometheus | grep -cE 'stockflow_stage_seconds|stockflow_consumer_lag')"
  ;;

restore)
  docker stop stockflow-realtime 2>/dev/null || true
  docker rm stockflow-realtime 2>/dev/null || true
  docker rename stockflow-realtime-p1 stockflow-realtime
  docker start stockflow-realtime
  echo ">> 원복 완료 (구 컨테이너 재가동). 토픽 파티션은 12로 유지(축소 불가)."
  ;;

status)
  docker ps -a --filter name=stockflow-realtime --format '{{.Names}}\t{{.Image}}\t{{.Status}}'
  docker exec stockflow-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic market.normalized 2>/dev/null | head -1
  docker image inspect "$IMG" --format 'built image: {{.Id}} {{.Created}}' 2>/dev/null || echo "built image: 없음"
  ;;
esac
