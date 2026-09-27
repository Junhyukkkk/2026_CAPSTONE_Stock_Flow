"""캔들 적재: 임시 테이블에 COPY 한 뒤 한 번에 병합한다.

EXCHANGE 캔들은 LIVE 를 덮어쓰고, 같은 EXCHANGE 도 최신 값으로 갱신한다.
"""
import io
import logging
import time
from datetime import datetime, timezone

from psycopg2.errors import DeadlockDetected
from sqlalchemy import text

from .db import get_engine

log = logging.getLogger(__name__)

# 거래소 아카이브 파일에 같은 분이 두 번 들어 있는 경우가 있어(예: VIRTUALUSDT 2026-07) 분당 한 행만 남긴다.
# 행을 시각 순으로 넣어 실시간 동기화와 같은 순서로 잠그므로 교착 상태가 생기지 않는다.
_MERGE_1M = """
    INSERT INTO ohlcv_1m (symbol, source, bucket, open, high, low, close, volume,
                          quote_volume, trade_count, origin, updated_at)
    SELECT symbol, source, bucket, open, high, low, close, volume, quote_volume, trade_count,
           'EXCHANGE', now()
    FROM (SELECT DISTINCT ON (symbol, source, bucket) *
          FROM stage_1m
          ORDER BY symbol, source, bucket, trade_count DESC NULLS LAST) s
    ORDER BY symbol, source, bucket
    ON CONFLICT (symbol, source, bucket) DO UPDATE
    SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
        volume = EXCLUDED.volume, quote_volume = EXCLUDED.quote_volume,
        trade_count = EXCLUDED.trade_count, origin = 'EXCHANGE', updated_at = now()
    WHERE ohlcv_1m.origin = 'LIVE'
       OR (ohlcv_1m.open, ohlcv_1m.high, ohlcv_1m.low, ohlcv_1m.close, ohlcv_1m.volume,
           ohlcv_1m.trade_count)
          IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close,
                            EXCLUDED.volume, EXCLUDED.trade_count)
"""

_MERGE_1D = """
    INSERT INTO symbol_daily_ohlcv (symbol, trade_date, market_type, source, open, high, low, close,
                                    volume, tick_count, computed_at, origin)
    SELECT symbol, (bucket AT TIME ZONE 'UTC')::date, %(market_type)s, source, open, high, low, close,
           volume, trade_count,
           now(), 'EXCHANGE'
    FROM (SELECT DISTINCT ON (symbol, source, bucket) *
          FROM stage_1m
          ORDER BY symbol, source, bucket, trade_count DESC NULLS LAST) s
    ORDER BY symbol, source, bucket
    ON CONFLICT (symbol, trade_date, source) DO UPDATE
    SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
        volume = EXCLUDED.volume, tick_count = EXCLUDED.tick_count, computed_at = now(),
        origin = 'EXCHANGE', market_type = EXCLUDED.market_type
"""


def _copy_stage(raw_conn, symbol: str, source: str, candles) -> None:
    buf = io.StringIO()
    for c in candles:
        buf.write(f"{symbol}\t{source}\t{c.open_time.isoformat()}\t{c.open}\t{c.high}\t{c.low}\t"
                  f"{c.close}\t{c.volume}\t{c.quote_volume}\t{c.trade_count}\n")
    buf.seek(0)
    with raw_conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS stage_1m (
                symbol VARCHAR(32), source VARCHAR(64), bucket TIMESTAMPTZ,
                open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume NUMERIC,
                quote_volume NUMERIC, trade_count BIGINT) ON COMMIT DROP""")
        cur.copy_expert("COPY stage_1m FROM STDIN", buf)


def is_deadlock(error) -> bool:
    """psycopg2 원본 예외든 SQLAlchemy 로 감싼 예외든 교착 상태인지 판별한다."""
    return isinstance(error, DeadlockDetected) or isinstance(getattr(error, "orig", None), DeadlockDetected)


def retry_on_deadlock(fn, attempts: int = 3, pause_seconds: float = 1.0):
    """교착 상태로 롤백된 트랜잭션은 다시 시도하면 대개 성공한다."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as e:
            if not is_deadlock(e) or attempt == attempts:
                raise
            log.warning("deadlock detected, retrying (%d/%d)", attempt, attempts - 1)
            time.sleep(pause_seconds * attempt)


def _merge(candles, symbol: str, source: str, sql: str, params=None) -> int:
    if not candles:
        return 0
    return retry_on_deadlock(lambda: _merge_once(candles, symbol, source, sql, params))


def _merge_once(candles, symbol: str, source: str, sql: str, params=None) -> int:
    """COPY 와 병합을 한 트랜잭션에서 수행하고 삽입·갱신된 행 수를 반환한다."""
    raw = get_engine().raw_connection()
    try:
        _copy_stage(raw.driver_connection, symbol, source, candles)
        with raw.driver_connection.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.rowcount
        raw.commit()
        return rows
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def upsert_minutes(symbol: str, source: str, candles) -> int:
    return _merge(candles, symbol, source, _MERGE_1M)


def upsert_daily(symbol: str, source: str, market_type: str, candles) -> int:
    return _merge(candles, symbol, source, _MERGE_1D, {"market_type": market_type})


def update_coverage(symbol: str, source: str, interval_code: str, status: str = None, note: str = None):
    """저장된 캔들 범위와 결측 수를 다시 계산해 data_coverage 에 기록한다.

    결측 수는 첫 캔들~마지막 캔들 사이 '연속 시장' 기준(코인: 24시간)이다.
    """
    if interval_code == "1m":
        q = """SELECT min(bucket), max(bucket), count(*) FROM ohlcv_1m
               WHERE symbol = :s AND source = :src"""
        step_seconds = 60
    else:
        q = """SELECT min(trade_date)::timestamptz, max(trade_date)::timestamptz, count(*)
               FROM symbol_daily_ohlcv WHERE symbol = :s AND source = :src"""
        step_seconds = 86400
    with get_engine().begin() as conn:
        first, last, count = conn.execute(text(q), {"s": symbol, "src": source}).one()
        missing = None
        if first is not None:
            expected = int((last - first).total_seconds() // step_seconds) + 1
            missing = max(0, expected - count)
        resolved = status or ("UNKNOWN" if first is None else ("COMPLETE" if missing == 0 else "GAPS"))
        conn.execute(text("""
            INSERT INTO data_coverage (symbol, source, interval_code, first_ts, last_ts, row_count,
                                       missing_count, last_checked_at, status, note)
            VALUES (:s, :src, :i, :f, :l, :c, :m, :now, :st, :note)
            ON CONFLICT (symbol, source, interval_code) DO UPDATE
            SET first_ts = EXCLUDED.first_ts, last_ts = EXCLUDED.last_ts, row_count = EXCLUDED.row_count,
                missing_count = EXCLUDED.missing_count, last_checked_at = EXCLUDED.last_checked_at,
                status = EXCLUDED.status, note = coalesce(EXCLUDED.note, data_coverage.note)"""),
            {"s": symbol, "src": source, "i": interval_code, "f": first, "l": last, "c": count,
             "m": missing, "now": datetime.now(timezone.utc), "st": resolved, "note": note})
    return first, last, count, missing
