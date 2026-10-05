"""포아송 도착 기반 체결 생성기"""
import random
from decimal import Decimal
from typing import List, Optional, Sequence

from normalizer import NormalizedTradeDTO
from simulator.market_clock import us_intraday_profile
from simulator.price_model import PriceModel, poisson
from simulator.universe import Instrument

SOURCE = 'SIMULATOR'
EXCHANGE = 'SIM'
MARKET_TYPE = 'STOCK'

SESSION_SECONDS = 23400.0  # 정규장 6.5시간
DAY_SECONDS = 86400.0
# daily_trades 가 없는 종목의 폴백: weight 1 당 하루 체결 수
DAILY_TRADES_PER_WEIGHT = 10_000.0

BASE36 = '0123456789abcdefghijklmnopqrstuvwxyz'


def to_base36(n: int) -> str:
    digits = ''
    while True:
        n, r = divmod(n, 36)
        digits = BASE36[r] + digits
        if n == 0:
            return digits


class TradeGenerator:
    def __init__(
        self,
        instruments: Sequence[Instrument],
        total_tps: float,
        run_id: str,
        seed: Optional[int] = None,
        rate_mode: str = 'fixed',
        rate_scale: float = 1.0,
        market_hours: str = 'always',
    ):
        """rate_mode='fixed': 종목 i 의 발생률 = total_tps * weight_i / Σweight (강도 곡선 없음).
        rate_mode='realistic': 평균 발생률 = daily_trades_i / 23400 * rate_scale (total_tps 무시),
        market_hours='us' 이면 거기에 장중 강도 곡선을 곱하고 'always' 는 평균으로 일정하게 낸다."""
        self._rng = random.Random(seed)
        self._run_id = run_id
        self._symbols = [i.symbol for i in instruments]
        self._profiled = rate_mode == 'realistic' and market_hours == 'us'
        # 가격 모델은 "체결 수 N 개가 하루 변동성 vol/sqrt(252) 를 이루도록" 체결 간 시간을 환산한다.
        # always 는 장 시간 없이 24시간 흐르므로 같은 건수가 정규장의 3.7배 시간에 퍼진다 — 환산 시간을 줄여 일 변동성을 유지한다.
        vol_time = 1.0
        if rate_mode == 'realistic':
            self._rates = [
                (i.daily_trades or i.weight * DAILY_TRADES_PER_WEIGHT) / SESSION_SECONDS * rate_scale
                for i in instruments
            ]
            if market_hours == 'always':
                vol_time = SESSION_SECONDS / DAY_SECONDS
        else:
            total_weight = sum(i.weight for i in instruments)
            self._rates = [total_tps * i.weight / total_weight for i in instruments]
        self._models = [
            PriceModel(i.price, i.volatility, vol_time / rate, self._rng)
            for i, rate in zip(instruments, self._rates)
        ]
        self._seq = [0] * len(instruments)

    @property
    def expected_tps(self) -> float:
        """전 종목 합계 평균 초당 체결 수 (강도 곡선 평균은 1.0 이므로 곡선과 무관)"""
        return sum(self._rates)

    def generate(self, t_start: float, t_end: float) -> List[NormalizedTradeDTO]:
        """[t_start, t_end) (epoch 초) 구간에 도착한 체결을 timestamp 순으로 반환한다. receivedAt 은 t_end."""
        dt = t_end - t_start
        if dt <= 0:
            return []

        intensity = us_intraday_profile((t_start + t_end) / 2) if self._profiled else 1.0
        if intensity <= 0:
            return []

        rng = self._rng
        received_at = int(t_end * 1000)
        trades: List[NormalizedTradeDTO] = []

        for idx, symbol in enumerate(self._symbols):
            count = poisson(rng, self._rates[idx] * dt * intensity)
            if count == 0:
                continue
            model = self._models[idx]
            # 같은 종목 안에서 시간 순서대로 가격이 움직이도록 도착 시각을 먼저 정렬한다
            for t in sorted(rng.uniform(t_start, t_end) for _ in range(count)):
                self._seq[idx] += 1
                trades.append(NormalizedTradeDTO(
                    source=SOURCE,
                    symbol=symbol,
                    price=Decimal(model.next_price_cents()).scaleb(-2),
                    volume=Decimal(model.next_volume()),
                    trade_id=f"SIM-{symbol}-{self._run_id}-{self._seq[idx]}",
                    exchange=EXCHANGE,
                    timestamp=int(t * 1000),
                    received_at=received_at,
                    market_type=MARKET_TYPE,
                ))

        trades.sort(key=lambda trade: trade.timestamp)
        return trades
