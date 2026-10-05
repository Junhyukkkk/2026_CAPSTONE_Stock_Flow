"""
미국 주식 시세 시뮬레이터 — 가짜 체결을 만들어 Kafka 로 전송한다.

⚠️ 생성되는 모든 데이터는 시뮬레이션이다 (source=SIMULATOR, exchange=SIM).
실제 시세가 아니므로 분석/백테스트 결과 해석에 주의할 것.
"""
import asyncio
import json
import logging
import os
import secrets
import signal
import sys
import time
from typing import Callable, Dict, List, Optional, Sequence

from config import Config
from simulator.generator import BASE36, SOURCE as DEFAULT_SOURCE_LABEL, TradeGenerator, to_base36
from simulator.market_clock import is_us_market_open
from simulator.price_seed import seed_prices
from simulator.universe import Instrument, load_universe

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 프로세스 정지/시스템 절전 뒤 한꺼번에 몰아서 쏟아내지 않도록 한 번에 따라잡는 최대 구간
MAX_CATCHUP_SEC = 2.0
CLOSED_POLL_SEC = 1.0
# 종료 시 달성률 경고 기준: 평균 속도 비교가 의미 있도록 최소 활성 시간과 목표 대비 하한
SHORTFALL_MIN_ELAPSED_SEC = 10.0
SHORTFALL_RATIO = 0.9


def shortfall_warning(
    sent: int,
    expected_tps: Optional[float],
    elapsed_sec: float,
    dry_run: bool = False,
) -> Optional[str]:
    """실제 전송 건수가 목표(expected_tps x 경과 초)의 90% 미만이면 경고 문구, 아니면 None.

    dry-run·목표 속도 미상(<=0/None)·경과 10초 미만이면 판단하지 않는다.
    """
    if dry_run or not expected_tps or expected_tps <= 0 or elapsed_sec < SHORTFALL_MIN_ELAPSED_SEC:
        return None
    target = expected_tps * elapsed_sec
    if sent >= target * SHORTFALL_RATIO:
        return None
    return (
        f"목표 발생률을 달성하지 못했습니다: 전송 {sent:,}건 / 목표 {target:,.0f}건 "
        f"(달성률 {sent / target * 100:.1f}%, 부족 {target - sent:,.0f}건, 평균 {sent / elapsed_sec:,.1f} msg/s "
        f"< 목표 {expected_tps:,.1f} msg/s). 단일 프로세스 Kafka 전송 한계일 수 있으니 "
        f"SIM_RATE_SCALE 을 나눠 시뮬레이터를 여러 개 띄우세요"
    )


class StockSimulator:
    def __init__(
        self,
        instruments: Sequence[Instrument],
        total_tps: float,
        market_hours: str = 'always',
        seed: Optional[int] = None,
        tick_interval_ms: int = 50,
        rate_mode: str = 'fixed',
        rate_scale: float = 1.0,
        source_label: str = DEFAULT_SOURCE_LABEL,
        dry_run: bool = False,
        producer=None,
        clock: Callable[[], float] = time.time,
    ):
        self._clock = clock
        self._market_hours = market_hours
        self._tick_sec = tick_interval_ms / 1000.0
        self._dry_run = dry_run
        self._producer = producer
        # 무작위 접미사는 시드 RNG 와 무관한 secrets 로 — 같은 ms 에 동시에 기동해도 tradeId 가 겹치지 않는다
        run_id = to_base36(int(clock() * 1000)) + ''.join(secrets.choice(BASE36) for _ in range(4))
        self._generator = TradeGenerator(
            instruments, total_tps, run_id=run_id, seed=seed,
            rate_mode=rate_mode, rate_scale=rate_scale, market_hours=market_hours,
            source_label=source_label,
        )
        self.expected_tps = self._generator.expected_tps
        # 달성률 경고용 활성 시간(장 열린 구간만) — us + realistic 은 강도 곡선 때문에 짧은 구간 평균이 목표와 다르므로 제외
        self._track_shortfall = not (market_hours == 'us' and rate_mode == 'realistic')
        self._active_sec = 0.0
        self._last_tick = clock()
        self._last_health_write = 0.0
        self.running = True
        self.market_open = True
        self.generated = 0
        self.rejected = 0

    def step(self) -> List[Dict]:
        """직전 step 이후 도착한 체결을 dict(NormalizedTradeDTO 형식) 목록으로 반환한다."""
        now = self._clock()
        t_start = max(self._last_tick, now - MAX_CATCHUP_SEC)
        self._last_tick = now

        if self._market_hours == 'us':
            # 장 경계를 걸친 구간은 통째로 버려, 장외 시각 체결이 나가지 않게 한다
            self.market_open = is_us_market_open(now)
            if not (self.market_open and is_us_market_open(t_start)):
                return []

        self._active_sec += now - t_start
        trades = [t.to_dict() for t in self._generator.generate(t_start, now)]
        self.generated += len(trades)
        return trades

    def _emit(self, trades: List[Dict]):
        if self._dry_run:
            sys.stdout.write(''.join(json.dumps(t) + '\n' for t in trades))
            sys.stdout.flush()
            self._update_health()
            return

        sent = False
        for trade in trades:
            if self._producer.produce(topic=Config.KAFKA_TOPIC_NAME, key=trade['symbol'], value=trade):
                sent = True
            else:
                self.rejected += 1
        if sent:
            self._update_health()
            self._producer.log_stats()

    def _update_health(self):
        """마지막 전송 성공 시각을 헬스체크 파일에 기록 (alpaca_producer._update_health 와 동일 방식)"""
        now = time.time()
        if now - self._last_health_write < Config.HEALTH_WRITE_MIN_INTERVAL_SEC:
            return
        self._last_health_write = now
        try:
            with open(Config.HEALTH_FILE_PATH, 'w') as f:
                json.dump({'last_success_epoch': now}, f)
        except OSError as e:
            logger.debug(f"헬스체크 파일 기록 실패: {e}")

    async def run(self):
        while self.running:
            trades = self.step()
            if trades:
                self._emit(trades)
            elif not self.market_open:
                # 장외는 정상 상태이므로 healthcheck 가 stale 로 판단하지 않게 생존 신호만 기록
                self._update_health()
            await asyncio.sleep(self._tick_sec if self.market_open else CLOSED_POLL_SEC)

    def stop(self, *_):
        self.running = False

    def shutdown(self):
        logger.info("🧹 종료 처리 시작...")
        self.running = False
        if self._producer:
            self._producer.close()
            metrics = self._producer.get_metrics()
            logger.info(
                f"📊 최종 통계 | "
                f"생성: {self.generated:,}건 | "
                f"전송: {metrics['total_sent']:,}건 | "
                f"실패: {metrics['total_failed']:,}건 | 큐 거부: {self.rejected:,}건 | "
                f"평균 속도: {metrics['messages_per_second']:.2f} msg/s | "
                f"성공률: {metrics['success_rate']*100:.2f}%"
            )
            warning = shortfall_warning(
                metrics['total_sent'],
                self.expected_tps if self._track_shortfall else None,
                self._active_sec,
                self._dry_run,
            )
            if warning:
                logger.warning(f"⚠️ {warning}")
        else:
            logger.info(f"📊 최종 통계 (dry-run) | 생성: {self.generated:,}건")
        logger.info("✅ 종료 완료")


def _resolve_path(path: str) -> str:
    return path if os.path.isabs(path) else os.path.join(BASE_DIR, path)


async def main():
    if not Config.validate() or not Config.validate_simulator():
        logger.error("❌ 설정 검증 실패. 종료합니다.")
        sys.exit(1)

    symbols_file = _resolve_path(Config.SIM_SYMBOLS_FILE)
    try:
        instruments = load_universe(symbols_file)
    except OSError as e:
        logger.error(f"❌ 종목 파일을 읽을 수 없습니다: {symbols_file} ({e})")
        sys.exit(1)
    if not instruments:
        logger.error(f"❌ 사용 가능한 종목이 없습니다: {symbols_file}")
        sys.exit(1)

    if Config.SIM_MARKET_HOURS == 'us':
        try:
            is_us_market_open(time.time())  # tzdata 누락 시 여기서 바로 실패
        except RuntimeError as e:
            logger.error(f"❌ {e}")
            sys.exit(1)
    instruments = seed_prices(
        instruments, Config.SIM_PRICE_SOURCE, Config.ALPACA_API_KEY, Config.ALPACA_API_SECRET
    )

    producer = None
    if not Config.SIM_DRY_RUN:
        from kafka_producer import KafkaProducerWrapper  # dry-run 은 Kafka 라이브러리 없이도 동작
        producer = KafkaProducerWrapper('simulator')

    simulator = StockSimulator(
        instruments,
        total_tps=Config.SIM_TOTAL_TPS,
        market_hours=Config.SIM_MARKET_HOURS,
        seed=Config.SIM_SEED,
        tick_interval_ms=Config.SIM_TICK_INTERVAL_MS,
        rate_mode=Config.SIM_RATE_MODE,
        rate_scale=Config.SIM_RATE_SCALE,
        source_label=Config.SIM_SOURCE_LABEL,
        dry_run=Config.SIM_DRY_RUN,
        producer=producer,
    )
    signal.signal(signal.SIGINT, simulator.stop)
    signal.signal(signal.SIGTERM, simulator.stop)

    mode = 'dry-run(stdout)' if Config.SIM_DRY_RUN else f'Kafka({Config.KAFKA_TOPIC_NAME})'
    if Config.SIM_RATE_MODE == 'realistic':
        rate_desc = (
            f"발생률: realistic ×{Config.SIM_RATE_SCALE:g} (SIM_TOTAL_TPS 무시됨) | "
            f"합계 평균 TPS: {simulator.expected_tps:,.1f}"
        )
    else:
        rate_desc = f"발생률: fixed | 총 TPS: {Config.SIM_TOTAL_TPS:g}"
    logger.info(
        f"🚀 주식 시뮬레이터 시작 (시뮬레이션 데이터 — 실제 시세 아님) | "
        f"source={Config.SIM_SOURCE_LABEL} | 종목: {len(instruments)}개 | {rate_desc} | "
        f"장 시간: {Config.SIM_MARKET_HOURS} | 모드: {mode} | seed: {Config.SIM_SEED}"
    )

    try:
        await simulator.run()
    except Exception as e:
        logger.error(f"❌ 치명적 오류: {e}", exc_info=True)
        simulator.shutdown()
        sys.exit(1)
    simulator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
