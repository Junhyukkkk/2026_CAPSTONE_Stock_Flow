import unittest
from datetime import date, datetime, timezone

from app.scripts.backfill_binance_daily import (
    parse_kline,
    select_missing_rows,
    validate_range,
)


class BinanceDailyBackfillTest(unittest.TestCase):
    def test_parse_kline_maps_daily_candle(self):
        open_time = int(
            datetime(2026, 8, 1, tzinfo=timezone.utc).timestamp() * 1000
        )
        row = parse_kline(
            "BTCUSDT",
            [
                open_time,
                "115000.00",
                "116000.00",
                "114000.00",
                "115500.00",
                "123.45",
                open_time + 86_399_999,
                "0",
                9876,
            ],
        )

        self.assertEqual(date(2026, 8, 1), row["trade_date"])
        self.assertEqual("CRYPTO", row["market_type"])
        self.assertEqual("BINANCE", row["source"])
        self.assertEqual(9876, row["tick_count"])
        self.assertEqual("115500.00", str(row["close"]))

    def test_parse_kline_rejects_inconsistent_high(self):
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            parse_kline(
                "BTCUSDT",
                [0, "10", "9", "8", "10", "1", 0, "0", 1],
            )

    def test_select_missing_rows_preserves_only_absent_dates(self):
        rows = [
            {"trade_date": date(2026, 8, 1)},
            {"trade_date": date(2026, 8, 2)},
        ]
        missing = select_missing_rows(rows, {date(2026, 8, 1)})
        self.assertEqual([date(2026, 8, 2)], [row["trade_date"] for row in missing])

    def test_validate_range_rejects_current_day(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            validate_range(
                date(2026, 9, 7),
                date(2026, 9, 8),
                today=date(2026, 9, 8),
            )


if __name__ == "__main__":
    unittest.main()
