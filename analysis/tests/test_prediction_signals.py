import unittest
from datetime import date

import numpy as np
import pandas as pd

from app.prediction_signals import (
    classify_signal,
    generate_walk_forward_signals,
    validate_crypto_daily_series,
)


class PredictionSignalsTest(unittest.TestCase):
    def setUp(self):
        index = pd.date_range("2026-01-01", periods=70, freq="1D", tz="UTC")
        self.series = pd.Series(np.linspace(100.0, 120.0, 70), index=index)

    def test_classify_signal_uses_symmetric_threshold(self):
        self.assertEqual("BUY", classify_signal(1.1, 1.0))
        self.assertEqual("HOLD", classify_signal(1.0, 1.0))
        self.assertEqual("HOLD", classify_signal(-1.0, 1.0))
        self.assertEqual("SELL", classify_signal(-1.1, 1.0))

    def test_walk_forward_uses_only_history_before_execution_date(self):
        calls = []

        def rising_forecaster(history, model, steps):
            calls.append((history.index[-1].date(), len(history), steps))
            last = float(history.iloc[-1])
            return np.array([last * (1.02 ** step) for step in range(1, steps + 1)])

        signals = generate_walk_forward_signals(
            self.series,
            model="ARIMA",
            from_date=date(2026, 2, 20),
            to_date=date(2026, 3, 1),
            warmup=30,
            refit_every=5,
            max_history=50,
            volatility_multiplier=0,
            fee_bps=0,
            slippage_bps=0,
            forecaster=rising_forecaster,
        )

        self.assertEqual(10, len(signals))
        self.assertEqual(date(2026, 2, 19), signals[0]["signal_date"])
        self.assertEqual(date(2026, 2, 20), signals[0]["execution_date"])
        self.assertTrue(all(point["signal"] == "BUY" for point in signals))
        self.assertEqual(date(2026, 2, 19), calls[0][0])
        self.assertEqual(5, calls[0][2])
        self.assertEqual(2, len(calls))

    def test_missing_crypto_date_is_rejected(self):
        series = self.series.drop(self.series.index[10])
        with self.assertRaisesRegex(ValueError, "missing dates"):
            validate_crypto_daily_series(series)

    def test_insufficient_warmup_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "observations"):
            generate_walk_forward_signals(
                self.series,
                model="ARIMA",
                from_date=date(2026, 1, 20),
                to_date=date(2026, 1, 25),
                warmup=30,
                forecaster=lambda history, model, steps: np.ones(steps),
            )


if __name__ == "__main__":
    unittest.main()
