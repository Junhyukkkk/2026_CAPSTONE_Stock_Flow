#!/usr/bin/env bash
# 시점별(git ref) 처리량 스윕 — "코드가 바뀌면서 처리 상한이 어떻게 변했나"
#
# ref 마다 그 시점의 코드로 앱 이미지를 빌드해 운영 중인 stockflow-realtime 자리에
# 바꿔 끼우고, tps-sweep.sh 로 같은 부하를 준 뒤, 원래 컨테이너로 복구한다.
# 인프라(Kafka 파티션·Redis·DB·서버)는 그대로 두고 코드만 바꾸므로 시점 간 차이는
# 코드 차이로 볼 수 있다.
#
#   ./revision-sweep.sh <label> <ref>[=rate,rate,...] [<ref>[=...] ...]
#   예) ./revision-sweep.sh march-to-now \
#         36400c1=1000,2000,3000,4000,5000 \
#         1f814a5=1000,2000,3000,4000,5000 \
#         30c7a5a=2000,4000,6000,8000,9000,10000
#
# rate 를 생략하면 RATES 환경변수(없으면 tps-sweep.sh 기본값)를 쓴다.
# HOLD / DRAIN_WAIT 등 나머지 환경변수는 tps-sweep.sh 로 그대로 전달된다.
# 어떤 ref 가 기동에 실패하면 그 ref 만 FAILED 로 기록하고 다음으로 넘어간다.
# 스크립트가 어떻게 끝나든(실패·Ctrl-C 포함) 원래 컨테이너는 복구된다.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL=${1:?"라벨을 지정하세요"}; shift
[ $# -ge 1 ] || { echo "ref 를 하나 이상 지정하세요" >&2; exit 1; }

REPO=${REPO:-/home/capstone01/capstone}
WT=${WT:-/home/capstone01/capstone-rev}
LIVE=${LIVE:-stockflow-realtime}
BACKUP="${LIVE}-live"
NETWORK=${NETWORK:-infra_default}
APP_URL=${APP_URL:-http://localhost:8081}
PAUSE_DURING_BUILD=${PAUSE_DURING_BUILD:-"stockflow-alpaca-collector stockflow-kafka-ui stockflow-redis-insight"}

OUT="$SCRIPT_DIR/results/revisions_${LABEL}_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
log() { echo -e "\033[1;35m[$(date +%H:%M:%S)]\033[0m $*"; }

# ── 복구: 시험용 컨테이너를 지우고 원래 컨테이너를 되살린다 ──────────────
restore() {
  set +e
  if docker ps -a --format '{{.Names}}' | grep -qx "$BACKUP"; then
    log "원래 컨테이너 복구"
    docker stop "$LIVE" >/dev/null 2>&1; docker rm "$LIVE" >/dev/null 2>&1
    docker rename "$BACKUP" "$LIVE"
    reset_offsets   # 시험 부하가 남긴 적체를 운영 앱이 떠안지 않게
    docker start "$LIVE" >/dev/null
  fi
  for c in $PAUSE_DURING_BUILD stockflow-binance-collector; do docker start "$c" >/dev/null 2>&1; done
  git -C "$REPO" worktree prune >/dev/null 2>&1
}
trap restore EXIT

# 두 컨슈머 그룹의 오프셋을 최신으로 맞춘다 (앱이 멈춘 상태에서만 가능).
# 시점마다 lag 0 에서 출발하게 해, 앞 시점이 남긴 적체가 다음 시점 측정에 섞이지 않게 한다.
reset_offsets() {
  local g
  for g in realtime-group storage-group; do
    for _ in $(seq 1 10); do
      if docker exec stockflow-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
           --group "$g" --topic market.normalized --reset-offsets --to-latest --execute >/dev/null 2>&1; then
        break
      fi
      sleep 3   # 그룹 멤버가 아직 빠져나가는 중이면 잠시 후 재시도
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

# ── 0. 원래 컨테이너의 env 를 그대로 물려준다 ─────────────────────────
# 이미지가 스스로 정하는 값(PATH, JAVA_HOME 등)만 빼고 전부 물려준다.
docker inspect "$LIVE" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -vE '^(PATH|HOME|HOSTNAME|JAVA_HOME|JAVA_VERSION|LANG|LC_ALL|TERM)=' | grep -v '^$' \
  > "$OUT/rt.env"
LIVE_IMAGE=$(docker inspect "$LIVE" --format '{{.Config.Image}}')
log "결과 → $OUT   (원래 이미지: $LIVE_IMAGE, env $(wc -l < "$OUT/rt.env")줄)"

{
  echo "# 시점별 스윕 — $LABEL  $(date -Is)"; echo
  echo "| ref | 날짜 | 커밋 | 최대 KEPT_UP | 첫 SATURATED | 결과 폴더 |"
  echo "| --- | --- | --- | ---: | ---: | --- |"
} > "$OUT/INDEX.md"

git -C "$REPO" fetch origin >/dev/null 2>&1 || true

for spec in "$@"; do
  ref=${spec%%=*}
  rates=""
  [ "$spec" != "$ref" ] && rates=$(echo "${spec#*=}" | tr ',' ' ')
  short=$(git -C "$REPO" rev-parse --short "$ref" 2>/dev/null) || { log "!! ref 해석 실패: $ref"; echo "| $ref | ? | ? | FAILED(ref) | | |" >> "$OUT/INDEX.md"; continue; }
  date_=$(git -C "$REPO" log -1 --format=%ad --date=short "$ref")
  subj=$(git -C "$REPO" log -1 --format=%s "$ref" | cut -c1-40)
  img="stockflow-app:rev-$short"
  log "════════ $ref → $short ($date_) $subj ════════"

  # 1. 그 시점 코드로 이미지 빌드
  if ! docker image inspect "$img" >/dev/null 2>&1; then
    rm -rf "$WT"; git -C "$REPO" worktree prune >/dev/null 2>&1
    git -C "$REPO" worktree add --detach "$WT" "$short" >/dev/null
    for c in $PAUSE_DURING_BUILD; do docker stop "$c" >/dev/null 2>&1 || true; done
  fi
  if docker image inspect "$img" >/dev/null 2>&1; then
    log "이미지 재사용: $img"
  elif ! { log "빌드: $img"; DOCKER_BUILDKIT=1 docker build -q -t "$img" -f "$WT/backend/Dockerfile" "$WT/backend" > "$OUT/$short.build.log" 2>&1; }; then
    log "!! 빌드 실패 → $OUT/$short.build.log"
    echo "| $short | $date_ | $subj | FAILED(build) | | |" >> "$OUT/INDEX.md"
    for c in $PAUSE_DURING_BUILD; do docker start "$c" >/dev/null 2>&1 || true; done
    continue
  fi
  for c in $PAUSE_DURING_BUILD; do docker start "$c" >/dev/null 2>&1 || true; done

  # 2. 앱 컨테이너 교체 (처음엔 원래 컨테이너를 BACKUP 이름으로 보관)
  if docker ps -a --format '{{.Names}}' | grep -qx "$BACKUP"; then
    docker stop "$LIVE" >/dev/null 2>&1 || true; docker rm "$LIVE" >/dev/null 2>&1 || true
  else
    docker stop "$LIVE" >/dev/null; docker rename "$LIVE" "$BACKUP"
  fi
  reset_offsets
  docker run -d --name "$LIVE" --network "$NETWORK" -p 8081:8081 \
    --env-file "$OUT/rt.env" "$img" >/dev/null
  if ! wait_up 300; then
    log "!! 기동 실패 (5분) → $OUT/$short.start.log"
    docker logs --tail 120 "$LIVE" > "$OUT/$short.start.log" 2>&1 || true
    echo "| $short | $date_ | $subj | FAILED(start) | | $short.start.log |" >> "$OUT/INDEX.md"
    continue
  fi
  sleep 20   # 컨슈머 리밸런스 안정화
  log "앱 UP — 스윕 시작 (rates: ${rates:-${RATES:-기본}})"

  # 3. 스윕
  set +e
  if [ -n "$rates" ]; then RATES="$rates" "$SCRIPT_DIR/tps-sweep.sh" "rev-$short" > "$OUT/$short.sweep.log" 2>&1
  else "$SCRIPT_DIR/tps-sweep.sh" "rev-$short" > "$OUT/$short.sweep.log" 2>&1; fi
  rc=$?
  set -e
  sweep_dir=$(ls -td "$SCRIPT_DIR"/results/sweep_rev-"$short"_* 2>/dev/null | head -1)
  if [ $rc -ne 0 ] || [ -z "$sweep_dir" ] || [ ! -f "$sweep_dir/SUMMARY.md" ]; then
    log "!! 스윕 실패 (rc=$rc) → $OUT/$short.sweep.log"
    echo "| $short | $date_ | $subj | FAILED(sweep) | | $short.sweep.log |" >> "$OUT/INDEX.md"
    continue
  fi
  cp "$sweep_dir/SUMMARY.md" "$OUT/$short.SUMMARY.md"
  kept=$(awk -F'|' '/KEPT_UP/ {gsub(/[ ,]/,"",$2); if ($2+0>m) m=$2+0} END {print m+0}' "$sweep_dir/SUMMARY.md")
  sat=$(awk -F'|' '/SATURATED/ {gsub(/[ ,]/,"",$2); print $2+0; exit}' "$sweep_dir/SUMMARY.md")
  echo "| $short | $date_ | $subj | ${kept:-0} | ${sat:-없음} | $(basename "$sweep_dir") |" >> "$OUT/INDEX.md"
  log "완료: 최대 KEPT_UP=${kept:-0}, 첫 SATURATED=${sat:-없음}"
done

log "전체 완료 → $OUT/INDEX.md"
cat "$OUT/INDEX.md"
