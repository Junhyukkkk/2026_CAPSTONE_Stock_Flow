#!/usr/bin/env bash
# 분석 서비스(FastAPI)가 쓸 CPU·메모리를 파이프라인과 분리해 예약한다. 컨테이너를 재생성하면 풀리므로
# `docker compose up` 뒤마다 다시 실행한다(idempotent). 8코어 기준 기본: 분석 = 코어 6,7 + 가중치 2배 + 메모리 3GB,
# 나머지 컨테이너 = 코어 0-5.
#   ANALYSIS_CPUSET=6,7 OTHER_CPUSET=0-5 ANALYSIS_MEM=3g ./reserve-analysis-capacity.sh
# 되돌리기: UNDO=1 ./reserve-analysis-capacity.sh  (cpuset 제한 해제)
set -euo pipefail

ANALYSIS_CONTAINER="${ANALYSIS_CONTAINER:-stockflow-analysis}"
ANALYSIS_CPUSET="${ANALYSIS_CPUSET:-6,7}"
OTHER_CPUSET="${OTHER_CPUSET:-0-5}"
ANALYSIS_MEM="${ANALYSIS_MEM:-3g}"

if [ "${UNDO:-0}" = "1" ]; then
  ALL="0-$(($(nproc) - 1))"
  for c in $(docker ps --format '{{.Names}}'); do docker update --cpuset-cpus "$ALL" "$c" >/dev/null; done
  docker update --cpu-shares 1024 --memory-reservation 0 --memory 0 --memory-swap -1 "$ANALYSIS_CONTAINER" >/dev/null
  echo "해제 완료 (cpuset=$ALL)"
  exit 0
fi

docker inspect "$ANALYSIS_CONTAINER" >/dev/null 2>&1 || { echo "컨테이너 없음: $ANALYSIS_CONTAINER" >&2; exit 1; }

for c in $(docker ps --format '{{.Names}}'); do
  [ "$c" = "$ANALYSIS_CONTAINER" ] && continue
  docker update --cpuset-cpus "$OTHER_CPUSET" "$c" >/dev/null
done
docker update --cpuset-cpus "$ANALYSIS_CPUSET" --cpu-shares 2048 \
  --memory-reservation 1g --memory "$ANALYSIS_MEM" --memory-swap "$ANALYSIS_MEM" "$ANALYSIS_CONTAINER" >/dev/null

echo "예약 완료: $ANALYSIS_CONTAINER cpuset=$ANALYSIS_CPUSET mem=$ANALYSIS_MEM / 그 외 컨테이너 cpuset=$OTHER_CPUSET"
