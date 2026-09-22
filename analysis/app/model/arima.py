"""ARIMA 학습/예측.

pmdarima 같은 무거운 의존성 없이 statsmodels 만으로 동작하도록,
작은 (p, d, q) 격자에서 AIC 가 가장 낮은 차수를 고른다.
"""
import itertools
import warnings

import numpy as np
from statsmodels.tsa.arima.model import ARIMA


def select_order(
    series,
    p_values=(0, 1, 2, 3),
    d_values=(0, 1),
    q_values=(0, 1, 2, 3),
):
    """AIC 기준 최적 (p, d, q) 탐색."""
    y = np.asarray(series, dtype=float)
    best_aic = np.inf
    best_order = (1, 1, 1)

    for p, d, q in itertools.product(p_values, d_values, q_values):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                res = ARIMA(y, order=(p, d, q)).fit()
            if np.isfinite(res.aic) and res.aic < best_aic:
                best_aic = res.aic
                best_order = (p, d, q)
        except Exception:
            continue

    return best_order, (None if np.isinf(best_aic) else float(best_aic))


def fit_arima(series, order=None):
    """ARIMA 적합. order 가 없으면 자동 선택."""
    if order is None:
        order, _ = select_order(series)
    y = np.asarray(series, dtype=float)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = ARIMA(y, order=tuple(order)).fit()
    return model, tuple(order)


def forecast(model, horizon: int):
    """horizon 스텝 예측. (평균, 95% 신뢰구간 ndarray[h, 2]) 반환."""
    fc = model.get_forecast(steps=horizon)
    mean = np.asarray(fc.predicted_mean, dtype=float)
    ci = np.asarray(fc.conf_int(alpha=0.05), dtype=float)
    return mean, ci
