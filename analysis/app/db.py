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

_MINUTE_RANGE_SQL = text(
    """
    SELECT bucket AS ts, open, high, low, close, volume
    FROM market_ticks_1m
    WHERE symbol = :symbol
      AND (:source IS NULL OR source = :source)
      AND bucket >= :from_time
      AND bucket < :to_time
    ORDER BY bucket ASC
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


def load_intraday_ohlcv_range(symbol: str, source, from_time, to_time) -> pd.DataFrame:
    """지정한 실제 시각 범위의 1분봉을 오래된 순으로 반환한다.

    워크포워드 백테스트는 최근 N개 봉이 아니라 사용자가 지정한 구간과 그 직전 학습 구간을
    정확히 읽어야 하므로, 일반 ``load_ohlcv`` 와 분리한다.
    """
    params = {
        "symbol": symbol,
        "source": source,
        "from_time": from_time,
        "to_time": to_time,
    }
    with get_engine().connect() as conn:
        df = pd.read_sql(_MINUTE_RANGE_SQL, conn, params=params)

    if df.empty:
        return df

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df.sort_values("ts").reset_index(drop=True)
