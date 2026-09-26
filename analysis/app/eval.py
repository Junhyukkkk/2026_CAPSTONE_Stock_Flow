"""예측 성능 평가 (MAE / RMSE) + 홀드아웃 백테스트."""
import numpy as np


def mae(actual, pred) -> float:
    a = np.asarray(actual, dtype=float)
    p = np.asarray(pred, dtype=float)
    return float(np.mean(np.abs(a - p)))


def rmse(actual, pred) -> float:
    a = np.asarray(actual, dtype=float)
    p = np.asarray(pred, dtype=float)
    return float(np.sqrt(np.mean((a - p) ** 2)))


def backtest_holdout(series, order=None, test_size=None):
    """뒤쪽 일부 구간을 테스트로 떼어 한 번에 예측한 뒤 오차 측정.

    데이터가 너무 적으면(None) 평가를 생략한다.
    """
    # 지연 import 로 순환참조 방지
    from .model.arima import fit_arima, forecast

    n = len(series)
    if test_size is None:
        test_size = max(10, int(n * 0.1))
    if n - test_size < 30:
        return None

    train = series.iloc[:-test_size]
    test = series.iloc[-test_size:]

    model, used_order = fit_arima(train, order=order)
    mean, _ = forecast(model, len(test))

    return {
        "mae": mae(test.values, mean),
        "rmse": rmse(test.values, mean),
        "test_size": int(test_size),
        "order": list(used_order),
    }
