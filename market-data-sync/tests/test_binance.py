import unittest
from datetime import date, datetime, timezone

from app.backfill import _minute_periods
from app.binance import archive_url, month_periods, parse_csv, to_datetime

# 실제 아카이브 형식: 2025 이전은 밀리초, 이후는 마이크로초
MS_ROW = ("1640995200000,46216.93,46271.08,46208.37,46250.00,40.57574,1640995259999,"
          "1875978.44,796,27.26,1260270.37,0")
US_ROW = ("1740787200000000,84349.95,84390.05,84324.42,84338.54,14.42832,1740787259999999,"
          "1217202.22,3153,8.16,688843.92,0")


class BinanceParsingTest(unittest.TestCase):
    def test_millisecond_and_microsecond_timestamps(self):
        self.assertEqual(to_datetime(1640995200000), datetime(2022, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(to_datetime(1740787200000000), datetime(2025, 3, 1, tzinfo=timezone.utc))

    def test_parse_csv_maps_columns_and_skips_header(self):
        candles = parse_csv("open_time,open,high\n" + MS_ROW + "\n" + US_ROW + "\n")
        self.assertEqual(len(candles), 2)
        first = candles[0]
        self.assertEqual(first.open_time, datetime(2022, 1, 1, tzinfo=timezone.utc))
        self.assertEqual((first.open, first.close, first.volume, first.trade_count),
                         ("46216.93", "46250.00", "40.57574", 796))
        self.assertEqual(candles[1].open_time, datetime(2025, 3, 1, tzinfo=timezone.utc))

    def test_archive_url_monthly_and_daily(self):
        self.assertTrue(archive_url("BTCUSDT", "1m", "2022-01").endswith(
            "/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2022-01.zip"))
        self.assertTrue(archive_url("BTCUSDT", "1m", "2026-09-24").endswith(
            "/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2026-09-24.zip"))

    def test_archive_url_encodes_non_ascii_symbols(self):
        url = archive_url("币安人生USDT", "1m", "2026-09")
        url.encode("ascii")  # urllib 은 ASCII URL 만 받는다
        self.assertIn("%E5%B8%81", url)

    def test_month_periods_cross_year(self):
        self.assertEqual(month_periods(date(2021, 11, 15), date(2022, 2, 1)),
                         ["2021-11", "2021-12", "2022-01", "2022-02"])

    def test_minute_periods_with_explicit_end_has_no_daily_files(self):
        self.assertEqual(_minute_periods("2022-01", "2022-03"), ["2022-01", "2022-02", "2022-03"])


if __name__ == "__main__":
    unittest.main()
