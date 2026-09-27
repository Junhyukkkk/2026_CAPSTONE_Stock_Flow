"""repair 통합 테스트. 일회용 TimescaleDB 필요 (MDS_INTEGRATION_DB=1).

외부 거래소 호출 대신 요청 구간을 채워 주는 가짜 fetch 를 쓴다(상장 시각 이전은 비워 둔다).
"""
import os
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import text

from app import repair
from app.binance import Candle
from app.db import get_engine

M = timedelta(minutes=1)
NOW = datetime(2026, 9, 20, 12, 0, 30, tzinfo=timezone.utc)
WINDOW_TO = datetime(2026, 9, 20, 11, 50, tzinfo=timezone.utc)     # now - 10분(정착 시간)
WINDOW_FROM = WINDOW_TO - timedelta(hours=6)                        # 기본 검사 범위 6시간 = 360분
GAPPY, FULL, DEAD, SILENT, NEW = ("REPAIRGAPUSDT", "REPAIRFULLUSDT", "REPAIRDEADUSDT",
                                  "REPAIRSILENTUSDT", "REPAIRNEWUSDT")
TRADING = {GAPPY, FULL, SILENT, NEW}


def fake_fetch(calls, listed_at=None):
    listed_at = listed_at or {}

    def fetch(symbol, interval, start, end):
        calls.append((symbol, interval, start, end))
        step = M if interval == "1m" else timedelta(days=1)
        out, t = [], max(start, listed_at.get(symbol, start))
        while t < end:
            out.append(Candle(open_time=t, open="1", high="2", low="1", close="1.5", volume="3",
                              close_time=t + step - timedelta(seconds=1), quote_volume="4", trade_count=7))
            t += step
        return out
    return fetch


@unittest.skipUnless(os.getenv("MDS_INTEGRATION_DB"), "needs a disposable TimescaleDB")
class RepairDbTest(unittest.TestCase):
    def setUp(self):
        with get_engine().begin() as c:
            for table in ("ohlcv_1m", "symbol_daily_ohlcv", "data_coverage"):
                c.execute(text(f"DELETE FROM {table} WHERE symbol LIKE 'REPAIR%'"))

    def _live(self, symbol, start, end, skip=()):
        with get_engine().begin() as c:
            c.execute(text("""
                INSERT INTO ohlcv_1m (symbol, source, bucket, open, high, low, close, volume, origin)
                SELECT :s, 'BINANCE', g, 9, 9, 9, 9, 9, 'LIVE'
                FROM generate_series(CAST(:a AS timestamptz), CAST(:b AS timestamptz) - interval '1 minute',
                                     interval '1 minute') g
                WHERE NOT (g = ANY(CAST(:skip AS timestamptz[])))"""),
                {"s": symbol, "a": start, "b": end, "skip": list(skip)})

    def _count(self, symbol, origin=None):
        q = "SELECT count(*) FROM ohlcv_1m WHERE symbol = :s" + (" AND origin = :o" if origin else "")
        with get_engine().connect() as c:
            return c.execute(text(q), {"s": symbol, "o": origin}).scalar()

    def test_refetches_whole_window_only_for_trading_symbols_with_gaps(self):
        self._live(GAPPY, WINDOW_FROM, WINDOW_TO, skip=[WINDOW_FROM + 100 * M, WINDOW_FROM + 101 * M,
                                                        WINDOW_FROM + 300 * M])
        self._live(FULL, WINDOW_FROM, WINDOW_TO)
        self._live(DEAD, WINDOW_FROM, WINDOW_TO, skip=[WINDOW_FROM + 5 * M])
        self._live(SILENT, WINDOW_FROM - timedelta(hours=3), WINDOW_FROM)   # 창 안에서 수집이 멈춤
        self._live(NEW, WINDOW_TO - 5 * M, WINDOW_TO)                       # 창 끝 5분 전에 상장
        listed = {NEW: WINDOW_TO - 5 * M}

        calls = []
        s = repair.repair_gaps(now=NOW, fetch=fake_fetch(calls, listed), trading=TRADING)

        self.assertEqual(sorted(c[0] for c in calls), sorted([GAPPY, SILENT, NEW]))
        self.assertTrue(all((c[2], c[3]) == (WINDOW_FROM, WINDOW_TO) for c in calls))
        self.assertEqual(s["symbols_checked"], 4)
        self.assertEqual(s["missing_minutes"], 3 + 360 + 355)
        self.assertEqual(s["unfillable"], 355)
        self.assertEqual(self._count(GAPPY, "EXCHANGE"), 360)   # 창 전체가 거래소 공식본으로
        self.assertEqual(self._count(SILENT, "EXCHANGE"), 360)
        self.assertEqual(self._count(FULL, "EXCHANGE"), 0)      # 빠진 게 없으면 요청하지 않는다
        self.assertEqual(self._count(DEAD), 359)                # 거래 중이 아니면 건드리지 않는다

        # 한 번 채운 구간은 다시 요청하지 않는다(상장 전 구간도 매시간 다시 묻지 않는다).
        calls = []
        repair.repair_gaps(now=NOW, fetch=fake_fetch(calls, listed), trading=TRADING)
        self.assertEqual(calls, [])

    def test_next_run_fetches_only_after_last_exchange_minute(self):
        self._live(GAPPY, WINDOW_FROM, WINDOW_TO, skip=[WINDOW_FROM + 10 * M])
        repair.repair_gaps(now=NOW, fetch=fake_fetch([]), trading={GAPPY})
        # 한 시간 뒤: 새 한 시간 분량이 LIVE 로 들어왔고 그중 한 분이 비어 있다.
        self._live(GAPPY, WINDOW_TO, WINDOW_TO + 60 * M, skip=[WINDOW_TO + 10 * M])
        calls = []
        s = repair.repair_gaps(now=NOW + timedelta(hours=1), fetch=fake_fetch(calls), trading={GAPPY})
        self.assertEqual(calls, [(GAPPY, "1m", WINDOW_TO, WINDOW_TO + 60 * M)])
        self.assertEqual(s["missing_minutes"], 1)

    def test_hours_argument_widens_window(self):
        self._live(GAPPY, WINDOW_FROM - timedelta(hours=10), WINDOW_TO,
                   skip=[WINDOW_FROM - timedelta(hours=5)])
        calls = []
        repair.repair_gaps(now=NOW, fetch=fake_fetch(calls), trading={GAPPY})
        self.assertEqual(calls, [])                              # 기본 6시간 창 밖의 결측
        repair.repair_gaps(now=NOW, fetch=fake_fetch(calls), trading={GAPPY}, hours=12)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][2], WINDOW_TO - timedelta(hours=12))

    def test_confirm_day_overwrites_live_and_writes_daily(self):
        day = date(2026, 9, 10)
        start = datetime(2026, 9, 10, tzinfo=timezone.utc)
        self._live(GAPPY, start + 60 * M, start + 70 * M)
        calls = []
        summary = repair.confirm_day(day, fetch=fake_fetch(calls), trading={GAPPY})

        self.assertEqual(summary["symbols"], 1)
        self.assertEqual(self._count(GAPPY, "EXCHANGE"), 1440)   # 하루 전체가 거래소 공식본으로
        self.assertEqual(self._count(GAPPY, "LIVE"), 0)
        with get_engine().connect() as c:
            row = c.execute(text("""SELECT close, origin FROM symbol_daily_ohlcv
                                    WHERE symbol = :s AND trade_date = :d"""),
                            {"s": GAPPY, "d": day}).one()
        self.assertEqual(row, (Decimal("1.5"), "EXCHANGE"))
        self.assertEqual(sorted({c[1] for c in calls}), ["1d", "1m"])


if __name__ == "__main__":
    unittest.main()
