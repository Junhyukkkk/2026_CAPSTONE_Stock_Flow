"""store 통합 테스트. 일회용 TimescaleDB 필요 (MDS_INTEGRATION_DB=1)."""
import os
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from app import store
from app.binance import Candle
from app.db import get_engine

SYMBOL = "STORETESTUSDT"
T0 = datetime(2024, 5, 1, tzinfo=timezone.utc)


def candle(minute, close="10", volume="1", trades=5):
    t = T0 + timedelta(minutes=minute)
    return Candle(open_time=t, open="10", high="11", low="9", close=close, volume=volume,
                  close_time=t + timedelta(seconds=59), quote_volume="10", trade_count=trades)


@unittest.skipUnless(os.getenv("MDS_INTEGRATION_DB"), "needs a disposable TimescaleDB")
class StoreDbTest(unittest.TestCase):
    def setUp(self):
        with get_engine().begin() as c:
            for table in ("ohlcv_1m", "symbol_daily_ohlcv", "data_coverage"):
                c.execute(text(f"DELETE FROM {table} WHERE symbol = :s"), {"s": SYMBOL})

    def _row(self, minute):
        with get_engine().connect() as c:
            return c.execute(text("""SELECT close, trade_count, origin FROM ohlcv_1m
                                     WHERE symbol = :s AND bucket = :b"""),
                             {"s": SYMBOL, "b": T0 + timedelta(minutes=minute)}).one()

    def test_exchange_overwrites_live_and_is_idempotent(self):
        with get_engine().begin() as c:
            c.execute(text("""INSERT INTO ohlcv_1m (symbol, source, bucket, open, high, low, close,
                                                    volume, origin)
                              VALUES (:s, 'BINANCE', :b, 1, 1, 1, 1, 1, 'LIVE')"""),
                      {"s": SYMBOL, "b": T0})
        candles = [candle(0, close="10.5"), candle(1)]
        self.assertEqual(store.upsert_minutes(SYMBOL, "BINANCE", candles), 2)
        self.assertEqual(self._row(0), (Decimal("10.5"), 5, "EXCHANGE"))
        # 같은 데이터로 다시 돌리면 아무것도 바뀌지 않는다.
        self.assertEqual(store.upsert_minutes(SYMBOL, "BINANCE", candles), 0)
        # 거래소 값이 정정되면 반영한다.
        self.assertEqual(store.upsert_minutes(SYMBOL, "BINANCE", [candle(1, close="12")]), 1)
        self.assertEqual(self._row(1)[0], Decimal("12"))

    def test_coverage_counts_missing_minutes(self):
        store.upsert_minutes(SYMBOL, "BINANCE", [candle(0), candle(1), candle(4)])
        first, last, count, missing = store.update_coverage(SYMBOL, "BINANCE", "1m")
        self.assertEqual((count, missing), (3, 2))
        with get_engine().connect() as c:
            status = c.execute(text("""SELECT status FROM data_coverage
                                       WHERE symbol = :s AND interval_code = '1m'"""),
                               {"s": SYMBOL}).scalar()
        self.assertEqual(status, "GAPS")

    def test_daily_upsert_sets_origin_and_utc_date(self):
        day = Candle(open_time=datetime(2024, 5, 1, tzinfo=timezone.utc), open="1", high="2",
                     low="0.5", close="1.5", volume="100",
                     close_time=datetime(2024, 5, 1, 23, 59, 59, tzinfo=timezone.utc),
                     quote_volume="150", trade_count=42)
        self.assertEqual(store.upsert_daily(SYMBOL, "BINANCE", "CRYPTO", [day]), 1)
        with get_engine().connect() as c:
            row = c.execute(text("""SELECT trade_date, close, tick_count, origin, market_type
                                    FROM symbol_daily_ohlcv WHERE symbol = :s"""),
                            {"s": SYMBOL}).one()
        self.assertEqual((str(row.trade_date), row.close, row.tick_count, row.origin, row.market_type),
                         ("2024-05-01", Decimal("1.5"), 42, "EXCHANGE", "CRYPTO"))


if __name__ == "__main__":
    unittest.main()
