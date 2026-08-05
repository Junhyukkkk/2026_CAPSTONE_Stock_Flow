#!/usr/bin/env bash
# 실시간 경로와 저장 경로의 결합도 검증
#
# storage consumer 를 끈 상태로 실시간 경로만 측정한다.
# 켠 상태(bench.sh 로 측정한 값)와 비교했을 때 실시간 E2E 가 개선되면
# 두 경로는 설계상 분리돼 있어도 실제로는 같은 JVM 의 CPU 와
# Redis 커넥션을 공유해 결합돼 있다는 뜻이다.
#
# 사용법: ./bench-nostorage.sh
# 환경변수: RATE(500) MEASURE_DURATION(120) WARMUP_DURATION(20) LINGER_MS(10)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/bench.sh" nostorage STORAGE_CONSUMER_ENABLED=false "$@"
