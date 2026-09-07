"""
병목 검증용 부하 생성기

collectors/kafka_producer.py 의 KafkaProducerWrapper 를 그대로 사용하므로
Producer 구간 설정(linger.ms, acks, compression.type)이 운영과 동일하다.

timestamp 를 전송 시각으로 넣기 때문에 Consumer 쪽 E2E latency 가
"Kafka 기록 -> Redis/WebSocket 도달" 구간을 그대로 반영한다.

환경변수:
  RATE           목표 초당 메시지 수 (기본 500) — 전체 WORKERS 합산 기준
  DURATION       전송 시간(초) (기본 60)
  SYMBOLS        심볼 개수 (기본 50)
  TOPIC          대상 토픽 (기본 market.normalized)
  WORKERS        송신 프로세스 수 (기본 1). 단일 프로세스로 균등 송신이
                 안 되는 고 RATE(대략 2,000+/s) 구간에서 2~8 로 올린다.
  BATCH_TICK_MS  0 이면 메시지마다 sleep 하는 원래 방식(기본, bench.sh 호환).
                 >0 이면 그 주기마다 (RATE*tick) 건씩 버스트로 흘린다.
                 고 RATE 에서 sleep 해상도 한계로 뒤처지는 것을 막는다.
"""
import logging
import os
import random
import time
from multiprocessing import Process, Queue

from kafka_producer import KafkaProducerWrapper

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('loadgen')

RATE = int(os.getenv('RATE', '500'))
DURATION = int(os.getenv('DURATION', '60'))
SYMBOL_COUNT = int(os.getenv('SYMBOLS', '50'))
TOPIC = os.getenv('TOPIC', 'market.normalized')
WORKERS = max(1, int(os.getenv('WORKERS', '1')))
BATCH_TICK_MS = int(os.getenv('BATCH_TICK_MS', '0'))

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


def _run_worker(worker_id: int, rate: int, result_q):
    """한 프로세스가 담당한 rate 를 DURATION 동안 흘린다."""
    producer = KafkaProducerWrapper(f'loadgen-{worker_id}')
    total = rate * DURATION
    # seq 를 worker 마다 어긋나게 시작해 tradeId 충돌을 피한다.
    seq = worker_id
    sent = 0
    start = time.time()

    if BATCH_TICK_MS > 0:
        tick = BATCH_TICK_MS / 1000.0
        per_tick = rate * tick
        carry = 0.0
        tick_idx = 0
        while True:
            elapsed = time.time() - start
            if elapsed >= DURATION:
                break
            carry += per_tick
            n = int(carry)
            carry -= n
            for _ in range(n):
                symbol = SYMBOLS[seq % SYMBOL_COUNT]
                producer.produce(TOPIC, symbol, make_message(symbol, seq))
                seq += WORKERS
                sent += 1
            tick_idx += 1
            target = start + tick_idx * tick
            now = time.time()
            if now < target:
                time.sleep(target - now)
    else:
        interval = 1.0 / rate
        i = 0
        while sent < total:
            target = start + i * interval
            now = time.time()
            if now < target:
                time.sleep(target - now)
            symbol = SYMBOLS[seq % SYMBOL_COUNT]
            producer.produce(TOPIC, symbol, make_message(symbol, seq))
            seq += WORKERS
            sent += 1
            i += 1

    producer.flush(timeout=30.0)
    elapsed = time.time() - start
    m = producer.get_metrics()
    result = {
        'worker': worker_id,
        'requested': sent,
        'acked': m['total_sent'],
        'failed': m['total_failed'],
        'elapsed': elapsed,
    }
    if result_q is None:
        return result
    result_q.put(result)


def main():
    per_worker = RATE // WORKERS
    remainder = RATE - per_worker * WORKERS
    rates = [per_worker + (1 if i < remainder else 0) for i in range(WORKERS)]

    logger.info(
        'loadgen 시작: topic=%s rate=%d/s workers=%d (worker당 %s) duration=%ds '
        'symbols=%d tick=%dms (총 %d건 예정)',
        TOPIC, RATE, WORKERS, rates, DURATION, SYMBOL_COUNT, BATCH_TICK_MS, RATE * DURATION,
    )

    if WORKERS == 1:
        results = [_run_worker(0, rates[0], None)]
    else:
        q = Queue()
        procs = [Process(target=_run_worker, args=(i, rates[i], q)) for i in range(WORKERS)]
        for p in procs:
            p.start()
        results = [q.get() for _ in procs]
        for p in procs:
            p.join()

    requested = sum(r['requested'] for r in results)
    acked = sum(r['acked'] for r in results)
    failed = sum(r['failed'] for r in results)
    elapsed = max(r['elapsed'] for r in results)

    logger.info(
        '완료: 요청 %d건 / 확인 %d건 / 실패 %d건 / %.1fs / 실효 %.1f msg/s (목표 %d)',
        requested, acked, failed, elapsed, acked / elapsed if elapsed else 0, RATE,
    )
    # tps-sweep.sh 가 파싱하는 한 줄 (KEY=VALUE)
    print(
        f'LOADGEN_RESULT requested={requested} acked={acked} failed={failed} '
        f'elapsed={elapsed:.1f} effective_rate={acked / elapsed if elapsed else 0:.1f} target_rate={RATE}'
    )


if __name__ == '__main__':
    main()
