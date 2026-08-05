"""
병목 검증용 부하 생성기

collectors/kafka_producer.py 의 KafkaProducerWrapper 를 그대로 사용하므로
Producer 구간 설정(linger.ms, acks, compression.type)이 운영과 동일하다.

timestamp 를 전송 시각으로 넣기 때문에 Consumer 쪽 E2E latency 가
"Kafka 기록 -> Redis/WebSocket 도달" 구간을 그대로 반영한다.

환경변수:
  RATE      초당 메시지 수 (기본 500)
  DURATION  전송 시간(초) (기본 60)
  SYMBOLS   심볼 개수 (기본 50)
  TOPIC     대상 토픽 (기본 market.normalized)
"""
import logging
import os
import random
import time

from kafka_producer import KafkaProducerWrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('loadgen')

RATE = int(os.getenv('RATE', '500'))
DURATION = int(os.getenv('DURATION', '60'))
SYMBOL_COUNT = int(os.getenv('SYMBOLS', '50'))
TOPIC = os.getenv('TOPIC', 'market.normalized')

SYMBOLS = [f'SYM{i:03d}' for i in range(SYMBOL_COUNT)]
BASE_PRICES = {s: random.uniform(10, 500) for s in SYMBOLS}


def make_message(symbol: str, seq: int) -> dict:
    now_ms = int(time.time() * 1000)
    drift = random.uniform(-0.002, 0.002)
    price = BASE_PRICES[symbol] * (1 + drift)
    return {
        'source': 'BINANCE',
        'symbol': symbol,
        'price': round(price, 4),
        'volume': round(random.uniform(0.01, 5.0), 4),
        'tradeId': f'{symbol}-{seq}',
        'exchange': 'BINANCE',
        'timestamp': now_ms,
        'receivedAt': now_ms,
        'marketType': 'CRYPTO',
    }


def main():
    logger.info(
        'loadgen 시작: topic=%s rate=%d/s duration=%ds symbols=%d (총 %d건 예정)',
        TOPIC, RATE, DURATION, SYMBOL_COUNT, RATE * DURATION,
    )

    producer = KafkaProducerWrapper('loadgen')

    # 메시지를 초 안에서 균등하게 흘린다.
    # 초마다 몰아서 보내면 뒤쪽 메시지가 버스트 뒤에 큐잉되어 E2E 지연이
    # 실제 병목이 아니라 부하 생성기 패턴 때문에 부풀려진다.
    total = RATE * DURATION
    interval = 1.0 / RATE
    seq = 0
    sent = 0
    start = time.time()
    next_log = 10.0

    while sent < total:
        target = start + seq * interval
        now = time.time()
        if now < target:
            time.sleep(target - now)

        symbol = SYMBOLS[seq % SYMBOL_COUNT]
        producer.produce(TOPIC, symbol, make_message(symbol, seq))
        seq += 1
        sent += 1

        elapsed = time.time() - start
        if elapsed >= next_log:
            logger.info('진행: %.0fs/%ds, 전송 %d건 (실효 %.1f msg/s)',
                        elapsed, DURATION, sent, sent / elapsed)
            next_log += 10.0

    producer.flush(timeout=30.0)
    total_elapsed = time.time() - start

    metrics = producer.get_metrics()
    logger.info(
        '완료: 요청 %d건 / 확인 %d건 / 실패 %d건 / %.1fs / 실효 %.1f msg/s',
        sent, metrics['total_sent'], metrics['total_failed'],
        total_elapsed, metrics['total_sent'] / total_elapsed,
    )


if __name__ == '__main__':
    main()
