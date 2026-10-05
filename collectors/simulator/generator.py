"""포아송 도착 기반 체결 생성기"""
import random
from decimal import Decimal
from typing import List, Optional, Sequence

from normalizer import NormalizedTradeDTO
from simulator.price_model import PriceModel, poisson
from simulator.universe import Instrument

SOURCE = 'SIMULATOR'
EXCHANGE = 'SIM'
MARKET_TYPE = 'STOCK'

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
    ):
        self._rng = random.Random(seed)
        self._run_id = run_id
        total_weight = sum(i.weight for i in instruments)
        self._symbols = [i.symbol for i in instruments]
        self._rates = [total_tps * i.weight / total_weight for i in instruments]
        self._models = [
            PriceModel(i.price, i.volatility, 1.0 / rate, self._rng)
            for i, rate in zip(instruments, self._rates)
        ]
        self._seq = [0] * len(instruments)

    def generate(self, t_start: float, t_end: float) -> List[NormalizedTradeDTO]:
        """[t_start, t_end) (epoch 초) 구간에 도착한 체결을 timestamp 순으로 반환한다. receivedAt 은 t_end."""
        dt = t_end - t_start
        if dt <= 0:
            return []

        rng = self._rng
        received_at = int(t_end * 1000)
        trades: List[NormalizedTradeDTO] = []

        for idx, symbol in enumerate(self._symbols):
            count = poisson(rng, self._rates[idx] * dt)
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
