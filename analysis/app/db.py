"""TimescaleDB에서 OHLCV 시계열을 읽어온다.

- 분봉(1m): 연속 집계 뷰 ``market_ticks_1m`` 사용
- 일봉(1d): 배치 집계 테이블 ``symbol_daily_ohlcv`` 사용
"""
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text

from .config import settings

_MINUTE_SQL = text(
    """
    SELECT bucket AS ts, open, high, low, close, volume
    FROM market_ticks_1m
    WHERE symbol = :symbol
      AND (:source IS NULL OR source = :source)
    ORDER BY bucket DESC
    LIMIT :limit
    """
)

_DAILY_SQL = text(
    """
    SELECT trade_date AS ts, open, high, low, close, volume
    FROM symbol_daily_ohlcv
    WHERE symbol = :symbol
      AND (:source IS NULL OR source = :source)
    ORDER BY trade_date DESC
    LIMIT :limit
    """
)


@lru_cache(maxsize=1)
def get_engine():
    return create_engine(settings.db_url, pool_pre_ping=True)


def load_ohlcv(symbol: str, interval: str = "1m", source=None, limit: int = 2000) -> pd.DataFrame:
    """오래된 순 → 최신 순으로 정렬된 OHLCV DataFrame 반환 (없으면 빈 DF)."""
    sql = _MINUTE_SQL if interval == "1m" else _DAILY_SQL
    params = {"symbol": symbol, "source": source, "limit": limit}
    with get_engine().connect() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    df = df.sort_values("ts").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df
