#!/usr/bin/env bash
# OptimizationProperties 런타임 토글(STOCKFLOW_OPT_*)을 하나씩 꺼서 개별 기여도를 뗀다.
# 코드/이미지는 고정(현재 운영 이미지)이고 env 하나만 바꿔 컨테이너를 재기동하므로
# revision-sweep.sh(시점마다 재빌드)보다 훨씬 빠르다.
#
#   ./opt-flag-sweep.sh <label> <rate> <flag_env_var> [<flag_env_var> ...]
#   예) ./opt-flag-sweep.sh optflags 7000 \
#         STOCKFLOW_OPT_REDIS_PIPELINE STOCKFLOW_OPT_INSTRUMENT_CACHE
#
# baseline(현재 env 그대로, 보통 전부 true)을 가장 먼저 재고, 그다음 플래그마다 그 값만
# false 로 바꿔서 같은 rate 로 잰다. HOLD/DRAIN_WAIT/SETTLE 은 환경변수로 조절 가능
# (기본 90/90/10 — revision-sweep 의 180/150/30 보다 짧게 잡아 여러 플래그를 빠르게 돈다).
# 스크립트가 어떻게 끝나든(실패·Ctrl-C 포함) 원래 컨테이너는 복구된다.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL=${1:?"라벨을 지정하세요"}; RATE=${2:?"rate 를 지정하세요"}; shift 2
[ $# -ge 1 ] || { echo "플래그 env 변수를 하나 이상 지정하세요" >&2; exit 1; }

LIVE=${LIVE:-stockflow-realtime}
BACKUP="${LIVE}-live"
NETWORK=${NETWORK:-infra_default}
APP_URL=${APP_URL:-http://localhost:8081}
HOLD=${HOLD:-90}
DRAIN_WAIT=${DRAIN_WAIT:-90}
SETTLE=${SETTLE:-10}

OUT="$SCRIPT_DIR/results/optflags_${LABEL}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
log() { echo -e "\033[1;33m[$(date +%H:%M:%S)]\033[0m $*"; }

restore() {
  set +e
  if docker ps -a --format '{{.Names}}' | grep -qx "$BACKUP"; then
    log "원래 컨테이너 복구"
    docker stop "$LIVE" >/dev/null 2>&1; docker rm "$LIVE" >/dev/null 2>&1
    docker rename "$BACKUP" "$LIVE"
    reset_offsets
    docker start "$LIVE" >/dev/null
  fi
  docker start stockflow-binance-collector >/dev/null 2>&1 || true
}
trap restore EXIT

# 두 컨슈머 그룹의 오프셋을 최신으로 맞춘다 (앱이 멈춘 상태에서만 가능).
reset_offsets() {
  local g
  for g in realtime-group storage-group; do
    for _ in $(seq 1 10); do
      docker exec stockflow-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$g" --topic market.normalized --reset-offsets --to-latest --execute >/dev/null 2>&1 && break
      sleep 3
    done
  done
  log "컨슈머 오프셋 최신으로 리셋 (lag 0 에서 시작)"
}

wait_up() {  # $1=timeout(s)
  local n=$(( $1 / 3 ))
  for _ in $(seq 1 "$n"); do
    curl -s --max-time 3 "$APP_URL/actuator/health" 2>/dev/null | grep -q '"UP"' && return 0
    sleep 3
  done
  return 1
}

IMAGE=$(docker inspect "$LIVE" --format '{{.Config.Image}}')
docker inspect "$LIVE" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -vE '^(PATH|HOME|HOSTNAME|JAVA_HOME|JAVA_VERSION|LANG|LC_ALL|TERM)=' | grep -v '^$' \
  > "$OUT/base.env"
log "베이스 이미지: $IMAGE, rate=$RATE, hold=${HOLD}s, env $(wc -l < "$OUT/base.env")줄 → $OUT"

# binance-collector 를 restore() 까지 계속 정지 — 실 트래픽이 오프셋 리셋 뒤에 새로 쌓여
# 첫 플래그 측정을 오염시키지 않게 (revision-sweep.sh 와 같은 이유).
docker stop stockflow-binance-collector >/dev/null 2>&1 || true

run_one() {  # $1=런이름  $2=env파일
  if docker ps -a --format '{{.Names}}' | grep -qx "$BACKUP"; then
    docker stop "$LIVE" >/dev/null 2>&1 || true; docker rm "$LIVE" >/dev/null 2>&1 || true
  else
    docker stop "$LIVE" >/dev/null; docker rename "$LIVE" "$BACKUP"
  fi
  reset_offsets
  docker run -d --name "$LIVE" --network "$NETWORK" -p 8081:8081 --env-file "$2" "$IMAGE" >/dev/null
  if ! wait_up 300; then
    log "!! 기동 실패 → $1"
    docker logs --tail 100 "$LIVE" > "$OUT/$1.start.log" 2>&1 || true
    return 1
  fi
  sleep 20   # 컨슈머 리밸런스 안정화
  set +e
  RATES="$RATE" HOLD="$HOLD" DRAIN_WAIT="$DRAIN_WAIT" SETTLE="$SETTLE" FORCE_CLI=1 \
    "$SCRIPT_DIR/tps-sweep.sh" "$1" > "$OUT/$1.sweep.log" 2>&1
  set -e
  sweep_dir=$(ls -td "$SCRIPT_DIR"/results/sweep_"$1"_* 2>/dev/null | head -1)
  if [ -n "$sweep_dir" ] && [ -f "$sweep_dir/SUMMARY.md" ]; then
    cp "$sweep_dir/SUMMARY.md" "$OUT/$1.SUMMARY.md"
    log "완료: $1 → $OUT/$1.SUMMARY.md"
  else
    log "!! 스윕 결과 없음 → $OUT/$1.sweep.log 확인"
  fi
}

log "════════ baseline (현재 env 그대로) ════════"
run_one baseline "$OUT/base.env"

for spec in "$@"; do
  # 기본은 false 로 끈다. "FLAG=true" 처럼 값을 지정하면 그 값으로 켠다 — baseline 에서
  # 이미 false 인 플래그(예: STORAGE_IDEMPOTENCY_PIPELINE)를 반대로 켜서 재는 용도.
  if [[ "$spec" == *::* ]]; then
    # "라벨::VAR=값;VAR2=값2" — 여러 환경변수를 한 번에 바꾼 조합을 한 런으로 잰다.
    name=${spec%%::*}; pairs=${spec#*::}
  elif [[ "$spec" == *=* ]]; then
    FLAG=${spec%%=*}; VAL=${spec#*=}; name="${VAL}_${FLAG#STOCKFLOW_OPT_}"; pairs="$spec"
  else
    FLAG=$spec; name="false_${FLAG#STOCKFLOW_OPT_}"; pairs="${FLAG}=false"
  fi
  cp "$OUT/base.env" "$OUT/$name.env"
  IFS=';' read -ra kvs <<< "$pairs"
  for kv in "${kvs[@]}"; do
    k=${kv%%=*}
    grep -v "^${k}=" "$OUT/$name.env" > "$OUT/$name.env.tmp" || true
    mv "$OUT/$name.env.tmp" "$OUT/$name.env"
    echo "$kv" >> "$OUT/$name.env"
  done
  log "════════ $name  ($pairs) ════════"
  run_one "$name" "$OUT/$name.env"
done

log "전체 완료 → $OUT"
