#!/usr/bin/env bash
# 분석 API 응답시간 프로브 — tps-sweep.sh 가 hold 구간 동안 백그라운드로 띄운다 (ANALYSIS_PROBE=1).
#   analysis_probe.sh <rate> <csv>
# INTERVAL 초마다 심볼을 순환하며 compare API 를 순차 1건씩 호출해 timestamp,rate,symbol,http,seconds 를 append.
# ANALYSIS_PROBE_WARMUP(기본 0, tps-sweep.sh 가 sim 모드에서만 10 으로 넘김)초 뒤에 첫 호출을 한다 —
# 부하 컨테이너가 뜨기 전(t=0) 표본이 섞여 응답시간이 낮게 나오는 편향을 막기 위함.
# SIGTERM/SIGINT 를 받으면 진행 중인 curl/sleep 까지 정리하고 종료한다(고아 프로세스 방지).
set -u
RATE=${1:?rate}
CSV=${2:?csv}
APP_URL=${APP_URL:-http://localhost:8081}
INTERVAL=${ANALYSIS_PROBE_INTERVAL:-15}
read -r -a SYMS <<< "${ANALYSIS_PROBE_SYMBOLS:-AAPL NVDA MSFT TSLA SPY}"
SOURCE=${ANALYSIS_PROBE_SOURCE:-SIMULATOR}
WARMUP=${ANALYSIS_PROBE_WARMUP:-0}
[ "${#SYMS[@]}" -gt 0 ] || exit 0

TMP=$(mktemp)
child=""
cleanup() { [ -n "$child" ] && kill "$child" 2>/dev/null; rm -f "$TMP"; }
trap 'cleanup; exit 0' TERM INT
trap cleanup EXIT

if [ "$WARMUP" -gt 0 ] 2>/dev/null; then sleep "$WARMUP" & child=$!; wait "$child" 2>/dev/null; child=""; fi
i=0
while true; do
  sym=${SYMS[$((i % ${#SYMS[@]}))]}; i=$((i + 1))
  # 백그라운드 + wait: bash 는 포그라운드 명령이 끝나야 trap 을 처리하므로 kill 에 즉시 반응하려면 필요
  curl -s -o /dev/null -w '%{http_code} %{time_total}' -m 120 \
    "$APP_URL/api/predictions/$sym/compare?interval=1m&horizon=5&source=$SOURCE" > "$TMP" 2>/dev/null &
  child=$!; wait "$child" 2>/dev/null; child=""
  read -r http secs < "$TMP" || true
  echo "$(date +%s),$RATE,$sym,${http:-000},${secs:-0}" >> "$CSV"
  : > "$TMP"
  sleep "$INTERVAL" & child=$!; wait "$child" 2>/dev/null; child=""
done
