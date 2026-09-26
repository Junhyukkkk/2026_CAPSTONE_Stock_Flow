"""live_sync 통합 테스트. 마이그레이션(V1~V16)이 적용된 일회용 TimescaleDB 가 필요하다.

    MDS_INTEGRATION_DB=1 DB_HOST=... python -m unittest tests.test_live_sync_db

운영 DB 에 돌리지 않도록 MDS_INTEGRATION_DB 가 없으면 건너뛴다.
"""
import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from app.db import get_engine
from app.live_sync import sync_range

T0 = datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)
SYMBOL = "TESTUSDT"


@unittest.skipUnless(os.getenv("MDS_INTEGRATION_DB"), "needs a disposable TimescaleDB")
class LiveSyncDbTest(unittest.TestCase):
    def setUp(self):
        with get_engine().begin() as c:
            c.execute(text("DELETE FROM market_ticks WHERE symbol = :s"), {"s": SYMBOL})
            c.execute(text("DELETE FROM ohlcv_1m WHERE symbol = :s"), {"s": SYMBOL})
        self._seq = 0

    def _tick(self, ts, price, volume="1"):
        self._seq += 1
        with get_engine().begin() as c:
            c.execute(text("""
                INSERT INTO market_ticks (source, symbol, trade_id, price, volume, exchange, ts,
                                          received_at, market_type)
                VALUES ('BINANCE', :s, :tid, :p, :v, 'BINANCE', :ts, :ts, 'CRYPTO')"""),
                {"s": SYMBOL, "tid": "t%d" % self._seq, "p": price, "v": volume, "ts": ts})

    def _refresh(self):
        # refresh_continuous_aggregate 는 트랜잭션 밖에서 호출해야 한다.
        with get_engine().connect().execution_options(isolation_level="AUTOCOMMIT") as c:
            c.execute(text("CALL refresh_continuous_aggregate('market_ticks_1m', :a, :b)"),
                      {"a": T0 - timedelta(hours=1), "b": T0 + timedelta(hours=1)})

    def _rows(self):
        with get_engine().connect() as c:
            return c.execute(text("""
                SELECT bucket, open, high, low, close, volume, origin FROM ohlcv_1m
                WHERE symbol = :s ORDER BY bucket"""), {"s": SYMBOL}).fetchall()

    def test_copies_settled_minutes_and_is_idempotent(self):
        self._tick(T0 + timedelta(seconds=5), "100")
        self._tick(T0 + timedelta(seconds=50), "102", "2")
        self._tick(T0 + timedelta(minutes=1, seconds=10), "101")
        self._refresh()

        self.assertEqual(sync_range(T0, T0 + timedelta(minutes=2)), 2)
        rows = self._rows()
        self.assertEqual([r.origin for r in rows], ["LIVE", "LIVE"])
        first = rows[0]
        self.assertEqual((first.open, first.high, first.low, first.close, first.volume),
                         (Decimal("100"), Decimal("102"), Decimal("100"), Decimal("102"), Decimal("3")))

        # 변화가 없으면 다시 돌려도 아무 행도 건드리지 않는다.
        self.assertEqual(sync_range(T0, T0 + timedelta(minutes=2)), 0)

    def test_late_tick_updates_live_row(self):
        self._tick(T0 + timedelta(seconds=5), "100")
        self._refresh()
        sync_range(T0, T0 + timedelta(minutes=1))

        self._tick(T0 + timedelta(seconds=30), "90")   # 늦게 도착한 틱
        self._refresh()
        self.assertEqual(sync_range(T0, T0 + timedelta(minutes=1)), 1)
        self.assertEqual(self._rows()[0].low, Decimal("90"))

    def test_never_overwrites_exchange_rows(self):
        with get_engine().begin() as c:
            c.execute(text("""
                INSERT INTO ohlcv_1m (symbol, source, bucket, open, high, low, close, volume, origin)
                VALUES (:s, 'BINANCE', :b, 1, 1, 1, 1, 1, 'EXCHANGE')"""), {"s": SYMBOL, "b": T0})
        self._tick(T0 + timedelta(seconds=5), "100")
        self._refresh()

        self.assertEqual(sync_range(T0, T0 + timedelta(minutes=1)), 0)
        row = self._rows()[0]
        self.assertEqual((row.origin, row.close), ("EXCHANGE", Decimal("1")))


if __name__ == "__main__":
    unittest.main()
