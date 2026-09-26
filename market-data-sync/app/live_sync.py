"""market_ticks_1m(연속 집계)의 확정된 분을 ohlcv_1m 에 LIVE 로 복사한다.

- 이미 EXCHANGE(거래소 확정본)가 있는 분은 건드리지 않는다.
- LIVE 끼리는 값이 바뀐 경우에만 갱신한다(늦게 도착한 틱으로 집계가 바뀐 경우).

수동 실행(과거 구간 시드):
    python -m app.live_sync --from 2026-03-29 --to 2026-09-26 --chunk-hours 24
"""
import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from .db import get_engine

log = logging.getLogger(__name__)

UPSERT_LIVE_SQL = text(
    """
    INSERT INTO ohlcv_1m (symbol, source, bucket, open, high, low, close, volume, origin, updated_at)
    SELECT symbol, source, bucket, open, high, low, close, volume, 'LIVE', now()
    FROM market_ticks_1m
    WHERE bucket >= :from_ts AND bucket < :to_ts
    ON CONFLICT (symbol, source, bucket) DO UPDATE
    SET open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        updated_at = now()
    WHERE ohlcv_1m.origin = 'LIVE'
      AND (ohlcv_1m.open, ohlcv_1m.high, ohlcv_1m.low, ohlcv_1m.close, ohlcv_1m.volume)
          IS DISTINCT FROM (EXCLUDED.open, EXCLUDED.high, EXCLUDED.low, EXCLUDED.close, EXCLUDED.volume)
    """
)


def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)


def sync_window(now: datetime, lookback_minutes: int, settle_minutes: int):
    """[from, to) 구간. to 는 아직 집계가 끝나지 않았을 수 있는 최근 분을 제외한다."""
    to_ts = floor_minute(now) - timedelta(minutes=settle_minutes)
    return to_ts - timedelta(minutes=lookback_minutes), to_ts


def sync_range(from_ts: datetime, to_ts: datetime) -> int:
    """구간을 한 번에 동기화하고 삽입·갱신된 행 수를 반환한다."""
    with get_engine().begin() as conn:
        result = conn.execute(UPSERT_LIVE_SQL, {"from_ts": from_ts, "to_ts": to_ts})
        return result.rowcount


def run_once(lookback_minutes: int, settle_minutes: int, now: datetime = None) -> int:
    now = now or datetime.now(timezone.utc)
    from_ts, to_ts = sync_window(now, lookback_minutes, settle_minutes)
    started = time.monotonic()
    rows = sync_range(from_ts, to_ts)
    log.info("live_sync %s..%s rows=%d took=%.2fs",
             from_ts.isoformat(), to_ts.isoformat(), rows, time.monotonic() - started)
    return rows


def seed(from_ts: datetime, to_ts: datetime, chunk_hours: int) -> int:
    """과거 구간을 chunk 단위로 나눠 복사한다(한 번에 큰 트랜잭션을 만들지 않기 위해)."""
    total = 0
    cursor = from_ts
    while cursor < to_ts:
        end = min(cursor + timedelta(hours=chunk_hours), to_ts)
        started = time.monotonic()
        rows = sync_range(cursor, end)
        total += rows
        log.info("seed %s..%s rows=%d took=%.2fs", cursor.isoformat(), end.isoformat(),
                 rows, time.monotonic() - started)
        cursor = end
    return total


def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


def main():
    parser = argparse.ArgumentParser(description="market_ticks_1m → ohlcv_1m(LIVE) 수동 시드")
    parser.add_argument("--from", dest="from_ts", required=True, type=_parse_ts)
    parser.add_argument("--to", dest="to_ts", required=True, type=_parse_ts)
    parser.add_argument("--chunk-hours", type=int, default=24)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    total = seed(args.from_ts, args.to_ts, args.chunk_hours)
    log.info("seed done rows=%d", total)


if __name__ == "__main__":
    main()
