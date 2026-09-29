"""연구용 일봉 패널 로딩 (읽기 전용).

봉인 구간(HOLDOUT_START 이후)은 기본적으로 읽지 않는다. 최종 시험(C3)에서만 allow_holdout=True 로 연다.
"""
from dataclasses import dataclass
from datetime import date

import pandas as pd
from sqlalchemy import text

from ..db import get_engine

HOLDOUT_START = date(2026, 8, 1)   # 사전 등록한 봉인 구간 시작 (2026-09-25 까지가 1회 시험 구간)
HOLDOUT_END = date(2026, 9, 25)


@dataclass
class Panel:
    """날짜 × 종목 행렬 모음. 결측일은 NaN 으로 두고 채우지 않는다."""
    open: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    close: pd.DataFrame
    volume: pd.DataFrame

    @property
    def quote_volume(self) -> pd.DataFrame:
        return self.close * self.volume


_SQL = text("""
    SELECT DISTINCT ON (symbol, trade_date) symbol, trade_date, open, high, low, close, volume
    FROM symbol_daily_ohlcv
    WHERE source = 'BINANCE' AND trade_date >= :start AND trade_date <= :end
    ORDER BY symbol, trade_date, computed_at DESC
""")


def load_panel(start: date = date(2019, 1, 1), end: date = None, allow_holdout: bool = False) -> Panel:
    end = end or (HOLDOUT_END if allow_holdout else date.fromordinal(HOLDOUT_START.toordinal() - 1))
    if not allow_holdout and end >= HOLDOUT_START:
        raise ValueError(f"end={end} reaches the sealed holdout (>= {HOLDOUT_START}); pass allow_holdout=True "
                         "only for the pre-registered final test")
    with get_engine().connect() as conn:
        df = pd.read_sql(_SQL, conn, params={"start": start, "end": end})
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    assert allow_holdout or df["trade_date"].max() < pd.Timestamp(HOLDOUT_START), "holdout leak"
    frames = {}
    for col in ("open", "high", "low", "close", "volume"):
        wide = df.pivot(index="trade_date", columns="symbol", values=col).astype(float)
        frames[col] = wide.asfreq("D")   # 빠진 날은 NaN (보간하지 않음)
    return Panel(**frames)
