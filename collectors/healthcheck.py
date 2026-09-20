"""
Docker HEALTHCHECK용 스크립트

각 Producer(binance_producer.py / alpaca_producer.py)는 메시지를 성공적으로
Kafka에 전송할 때마다 HEALTH_FILE_PATH에 마지막 성공 시각을 기록한다.
이 스크립트는 그 파일을 읽어, 마지막 성공 시각이 임계값(HEALTH_STALE_THRESHOLD_SEC)
보다 오래되지 않았으면 정상(exit 0), 그렇지 않으면 비정상(exit 1)으로 판단한다.

의도적으로 confluent_kafka/websockets 등 무거운 의존성을 import하지 않고
표준 라이브러리만 사용한다 (헬스체크 자체가 실패/지연되지 않도록).
"""
import json
import os
import sys
import time

HEALTH_FILE_PATH = os.getenv('HEALTH_FILE_PATH', '/tmp/collector_health.json')
STALE_THRESHOLD_SEC = int(os.getenv('HEALTH_STALE_THRESHOLD_SEC', '120'))


def main() -> int:
    if not os.path.exists(HEALTH_FILE_PATH):
        # 시작 직후(초기 종목 조회/인증 중)일 수 있으므로, Dockerfile의
        # HEALTHCHECK --start-period 가 이 구간을 흡수한다.
        print(f"health file not found yet: {HEALTH_FILE_PATH}", file=sys.stderr)
        return 1

    try:
        with open(HEALTH_FILE_PATH, 'r') as f:
            data = json.load(f)
        last_success_epoch = float(data['last_success_epoch'])
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as e:
        print(f"health file unreadable/invalid: {e}", file=sys.stderr)
        return 1

    age_sec = time.time() - last_success_epoch
    if age_sec > STALE_THRESHOLD_SEC:
        print(
            f"unhealthy: last successful message {age_sec:.1f}s ago "
            f"(threshold {STALE_THRESHOLD_SEC}s)",
            file=sys.stderr
        )
        return 1

    print(f"healthy: last successful message {age_sec:.1f}s ago")
    return 0


if __name__ == "__main__":
    sys.exit(main())
