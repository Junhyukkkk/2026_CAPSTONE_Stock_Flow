import unittest
from datetime import datetime, timezone

from app.live_sync import floor_minute, sync_window


class SyncWindowTest(unittest.TestCase):
    def test_window_excludes_unsettled_minutes(self):
        now = datetime(2026, 9, 26, 12, 9, 42, 123000, tzinfo=timezone.utc)
        from_ts, to_ts = sync_window(now, lookback_minutes=15, settle_minutes=2)
        self.assertEqual(to_ts, datetime(2026, 9, 26, 12, 7, tzinfo=timezone.utc))
        self.assertEqual(from_ts, datetime(2026, 9, 26, 11, 52, tzinfo=timezone.utc))

    def test_floor_minute_drops_seconds(self):
        ts = datetime(2026, 1, 1, 0, 0, 59, 999999, tzinfo=timezone.utc)
        self.assertEqual(floor_minute(ts), datetime(2026, 1, 1, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
