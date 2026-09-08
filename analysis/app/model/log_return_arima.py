"""Log-return ARIMA forecast utilities.

The model is fitted on log returns, but all public outputs are restored to
price scale so it can be compared directly with the existing price ARIMA.
"""
import numpy as np

from .arima import fit_arima, forecast as arima_forecast


def _to_log_returns(price_series):
    prices = price_series.astype(float).dropna()
    prices = prices[prices > 0]
    if len(prices) < 3:
        return prices, None

    returns = np.log(prices / prices.shift(1)).dropna()
    return prices, returns


def _returns_to_prices(last_price: float, predicted_returns):
    cumulative_returns = np.cumsum(np.asarray(predicted_returns, dtype=float))
    return last_price * np.exp(cumulative_returns)


def forecast(price_series, horizon: int, order=None):
    """Fit ARIMA on log returns and return restored price forecasts."""
    prices, returns = _to_log_returns(price_series)
    if returns is None or len(returns) < 30:
        return None

    if order is None:
        order = (1, 0, 1)
    model, used_order = fit_arima(returns, order=order)
    mean_returns, ci_returns = arima_forecast(model, horizon)

    last_price = float(prices.iloc[-1])
    mean_prices = _returns_to_prices(last_price, mean_returns)
    lower_prices = _returns_to_prices(last_price, ci_returns[:, 0])
    upper_prices = _returns_to_prices(last_price, ci_returns[:, 1])

    return mean_prices, lower_prices, upper_prices, list(used_order)


def backtest_holdout(price_series, order=None, test_size=None):
    """Evaluate log-return ARIMA on restored price scale."""
    prices = price_series.astype(float).dropna()
    prices = prices[prices > 0]

    n = len(prices)
    if test_size is None:
        test_size = min(30, max(10, int(n * 0.1)))
    if n - test_size < 31:
        return None

    train_prices = prices.iloc[:-test_size]
    test_prices = prices.iloc[-test_size:]

    forecast_result = forecast(train_prices, len(test_prices), order=order)
    if forecast_result is None:
        return None

    mean_prices, _, _, used_order = forecast_result
    actual = np.asarray(test_prices.values, dtype=float)
    predicted = np.asarray(mean_prices, dtype=float)

    return {
        "mae": float(np.mean(np.abs(actual - predicted))),
        "rmse": float(np.sqrt(np.mean((actual - predicted) ** 2))),
        "test_size": int(test_size),
        "order": used_order,
    }
