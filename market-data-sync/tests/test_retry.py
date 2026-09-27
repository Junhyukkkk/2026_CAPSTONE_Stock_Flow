import unittest
from unittest import mock

from psycopg2.errors import DeadlockDetected
from sqlalchemy.exc import OperationalError

from app.store import is_deadlock, retry_on_deadlock


class RetryOnDeadlockTest(unittest.TestCase):
    def test_detects_raw_and_wrapped_deadlocks(self):
        raw = DeadlockDetected()
        self.assertTrue(is_deadlock(raw))
        self.assertTrue(is_deadlock(OperationalError("stmt", {}, raw)))
        self.assertFalse(is_deadlock(ValueError("nope")))

    @mock.patch("app.store.time.sleep")
    def test_retries_deadlock_then_succeeds(self, _sleep):
        calls = []

        def fn():
            calls.append(1)
            if len(calls) < 3:
                raise DeadlockDetected()
            return 42

        self.assertEqual(retry_on_deadlock(fn), 42)
        self.assertEqual(len(calls), 3)

    @mock.patch("app.store.time.sleep")
    def test_gives_up_after_attempts_and_does_not_retry_other_errors(self, _sleep):
        with self.assertRaises(DeadlockDetected):
            retry_on_deadlock(lambda: (_ for _ in ()).throw(DeadlockDetected()), attempts=2)
        calls = []

        def boom():
            calls.append(1)
            raise ValueError("bad data")

        with self.assertRaises(ValueError):
            retry_on_deadlock(boom)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
