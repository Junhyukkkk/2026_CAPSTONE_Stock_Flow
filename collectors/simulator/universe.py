"""
시뮬레이션 종목 데이터(universe) 로딩

CSV 헤더: symbol,price,volatility,weight
  price      시작가(USD)
  volatility 연환산 변동성 (예: 0.28)
  weight     상대 체결 빈도(유동성)
"""
import csv
import logging
import math
from dataclasses import dataclass
from typing import List, Set

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Instrument:
    symbol: str
    price: float
    volatility: float
    weight: float


def load_universe(path: str) -> List[Instrument]:
    """CSV 를 읽어 종목 목록을 반환한다. 형식 오류·가격≤0·가중치≤0 행은 로그를 남기고 제외한다."""
    instruments: List[Instrument] = []
    seen: Set[str] = set()

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            line = reader.line_num
            try:
                symbol = (row['symbol'] or '').strip().upper()
                price = float(row['price'])
                volatility = float(row['volatility'])
                weight = float(row['weight'])
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"⚠️ universe {line}행 형식 오류로 건너뜀: {row} ({e!r})")
                continue

            if not symbol or not all(math.isfinite(v) for v in (price, volatility, weight)):
                logger.warning(f"⚠️ universe {line}행 값이 올바르지 않아 건너뜀: {row}")
                continue
            if price <= 0 or weight <= 0 or volatility < 0:
                logger.warning(f"⚠️ universe {line}행 가격/가중치/변동성 범위 오류로 제외: {row}")
                continue
            if symbol in seen:
                logger.warning(f"⚠️ universe {line}행 중복 심볼 제외: {symbol}")
                continue

            seen.add(symbol)
            instruments.append(Instrument(symbol, price, volatility, weight))

    return instruments
