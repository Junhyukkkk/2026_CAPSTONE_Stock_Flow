"""Walk-forward prediction signals for the Spring backtest engine."""

from __future__ import annotations

from datetime import date
from math import sqrt
from typing import Callable

import numpy as np
import pandas as pd

from .model.arima import fit_arima, forecast as arima_forecast
from .model.log_return_arima import forecast as log_return_forecast


PRICE_ARIMA = "ARIMA"
LOG_RETURN_ARIMA = "LOG_RETURN_ARIMA"
CHRONOS_BOLT = "CHRONOS_BOLT"
SUPPORTED_MODELS = {PRICE_ARIMA, LOG_RETURN_ARIMA, CHRONOS_BOLT}

Forecaster = Callable[[pd.Series, str, int], np.ndarray]


def forecast_prices(history: pd.Series, model: str, steps: int) -> np.ndarray:
    if model == PRICE_ARIMA:
        fitted, _ = fit_arima(history, order=(1, 1, 1))
        mean, _ = arima_forecast(fitted, steps)
        return np.asarray(mean, dtype=float)

    if model == LOG_RETURN_ARIMA:
        result = log_return_forecast(history, steps, order=(1, 0, 1))
        if result is None:
            raise ValueError("insufficient history for log-return ARIMA")
        mean, _, _, _ = result
        return np.asarray(mean, dtype=float)

    if model == CHRONOS_BOLT:
        from .model.chronos_bolt import forecast as chronos_forecast

        mean, _, _ = chronos_forecast(history, steps)
        return np.asarray(mean, dtype=float)

    raise ValueError(f"unsupported prediction model: {model}")


def volatility_pct(history: pd.Series, window: int) -> float:
    prices = np.asarray(history.tail(window + 1), dtype=float)
    if prices.size < 3 or np.any(prices <= 0):
        return 0.0
    returns = np.diff(np.log(prices))
    if returns.size < 2:
        return 0.0
    value = float(np.std(returns, ddof=1) * 100.0)
    return value if np.isfinite(value) else 0.0


def classify_signal(expected_return_pct: float, threshold_pct: float) -> str:
    if expected_return_pct > threshold_pct:
        return "BUY"
    if expected_return_pct < -threshold_pct:
        return "SELL"
    return "HOLD"


def validate_daily_series(
    series: pd.Series, *, require_consecutive_days: bool = True
) -> None:
    if series.empty:
        raise ValueError("daily close series is empty")
    if series.index.has_duplicates:
        raise ValueError("daily close series contains duplicate dates")
    if series.isna().any() or not np.isfinite(series.to_numpy(dtype=float)).all():
        raise ValueError("daily close series contains invalid values")
    if (series <= 0).any():
        raise ValueError("daily close prices must be positive")

    if require_consecutive_days:
        normalized = pd.DatetimeIndex(series.index).normalize()
        gaps = normalized.to_series().diff().dropna().dt.days
        if not gaps.empty and int(gaps.max()) > 1:
            raise ValueError("daily crypto series contains missing dates")


def validate_crypto_daily_series(series: pd.Series) -> None:
    """Backward-compatible strict validation for seven-day crypto markets."""
    validate_daily_series(series, require_consecutive_days=True)


def generate_walk_forward_signals(
    series: pd.Series,
    *,
    model: str,
    from_date: date,
    to_date: date,
    warmup: int = 50,
    refit_every: int = 5,
    max_history: int = 200,
    volatility_window: int = 20,
    volatility_multiplier: float = 0.5,
    fee_bps: float = 10,
    slippage_bps: float = 5,
    require_consecutive_days: bool = True,
    forecaster: Forecaster = forecast_prices,
) -> list[dict]:
    """Generate signals on prior closes for execution on subsequent daily bars."""
    if model not in SUPPORTED_MODELS:
        raise ValueError(f"unsupported prediction model: {model}")
    if from_date > to_date:
        raise ValueError("from_date must be earlier than or equal to to_date")
    if warmup < 30 or refit_every < 1 or max_history < warmup:
        raise ValueError("invalid warmup, refit_every, or max_history")

    series = series.astype(float).sort_index()
    validate_daily_series(
        series, require_consecutive_days=require_consecutive_days
    )
    dates = [timestamp.date() for timestamp in pd.DatetimeIndex(series.index)]
    target_indices = [
        index for index, day in enumerate(dates) if from_date <= day <= to_date
    ]
    if not target_indices:
        raise ValueError("no daily bars in requested range")

    first_index = target_indices[0]
    last_index = target_indices[-1]
    if first_index < warmup:
        raise ValueError(
            f"at least {warmup} observations are required before from_date"
        )

    round_trip_cost_pct = 2.0 * (fee_bps + slippage_bps) / 100.0
    signals: list[dict] = []
    execution_index = first_index

    while execution_index <= last_index:
        block_size = min(refit_every, last_index - execution_index + 1)
        history_start = max(0, execution_index - max_history)
        history = series.iloc[history_start:execution_index]
        if len(history) < warmup:
            raise ValueError("insufficient history for walk-forward forecast")

        predictions = np.asarray(
            forecaster(history, model, block_size), dtype=float
        )
        if predictions.size != block_size or not np.isfinite(predictions).all():
            raise ValueError("forecast returned invalid prediction values")

        reference_price = float(history.iloc[-1])
        daily_volatility_pct = volatility_pct(history, volatility_window)
        signal_date = dates[execution_index - 1]

        for offset in range(block_size):
            predicted_price = float(predictions[offset])
            if predicted_price <= 0:
                raise ValueError("forecast returned a non-positive price")

            step = offset + 1
            expected_return_pct = (predicted_price / reference_price - 1.0) * 100.0
            threshold_pct = round_trip_cost_pct + (
                volatility_multiplier * daily_volatility_pct * sqrt(step)
            )
            signals.append(
                {
                    "signal_date": signal_date,
                    "execution_date": dates[execution_index + offset],
                    "reference_price": reference_price,
                    "predicted_price": predicted_price,
                    "expected_return_pct": expected_return_pct,
                    "threshold_pct": threshold_pct,
                    "signal": classify_signal(expected_return_pct, threshold_pct),
                }
            )

        execution_index += block_size

    return signals
