"""학습/예측 오케스트레이션: DB 로드 → 전처리 → ARIMA → 평가/예측."""
from datetime import datetime, timezone

import pandas as pd

from . import db, eval as ev, preprocess
from .config import settings
from .model import store
from .model.arima import fit_arima, forecast as arima_forecast
from .model.chronos_bolt import forecast as chronos_forecast
from .model.log_return_arima import backtest_holdout as log_return_backtest_holdout
from .model.log_return_arima import forecast as log_return_forecast

MIN_OBS = 50  # ARIMA 학습 최소 관측치


def _load_clean(symbol: str, interval: str, source, limit: int):
    df = db.load_ohlcv(symbol, interval=interval, source=source, limit=limit)
    if df.empty:
        return None
    s = preprocess.to_close_series(df)
    freq = preprocess.FREQ.get(interval, "1min")
    s = preprocess.clean_series(s, freq=freq).dropna()
    return s


def train(symbol: str, interval: str = "1m", source=None, limit=None, order=None):
    """모델 학습 후 저장. 데이터 부족 시 None."""
    limit = limit or settings.default_limit
    s = _load_clean(symbol, interval, source, limit)
    if s is None or s.shape[0] < MIN_OBS:
        return None

    metrics = ev.backtest_holdout(s, order)
    model, used_order = fit_arima(s, order=order)

    payload = {
        "symbol": symbol,
        "interval": interval,
        "order": list(used_order),
        "metrics": metrics,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "last_ts": s.index[-1].isoformat(),
        "last_value": float(s.iloc[-1]),
        "n_obs": int(s.shape[0]),
        "freq": preprocess.FREQ.get(interval, "1min"),
        "model": model,
    }
    store.save_model(symbol, interval, payload)
    return payload


def predict(symbol: str, interval: str = "1m", horizon: int = 10,
            source=None, limit=None, retrain: bool = False):
    """예측 반환. 저장 모델이 없거나 retrain=True 면 즉석 학습."""
    payload = None if retrain else store.load_model(symbol, interval)
    if payload is None:
        payload = train(symbol, interval, source, limit, None)
        if payload is None:
            return None

    if payload.get("last_value") is None:
        payload["last_value"] = _lookup_last_value(
            symbol, interval, source, limit or settings.default_limit, payload["last_ts"]
        )

    model = payload["model"]
    mean, ci = arima_forecast(model, horizon)

    freq = payload.get("freq", "1min")
    last_ts = pd.Timestamp(payload["last_ts"])
    future_idx = pd.date_range(start=last_ts, periods=horizon + 1, freq=freq)[1:]

    points = [
        {
            "ts": future_idx[i].isoformat(),
            "yhat": float(mean[i]),
            "yhat_lower": float(ci[i, 0]),
            "yhat_upper": float(ci[i, 1]),
        }
        for i in range(horizon)
    ]

    return {
        "symbol": symbol,
        "interval": interval,
        "order": payload["order"],
        "metrics": payload.get("metrics"),
        "trained_at": payload["trained_at"],
        "last_ts": payload["last_ts"],
        "last_value": payload.get("last_value"),
        "horizon": horizon,
        "forecast": points,
    }


def predict_compare(symbol: str, interval: str = "1m", horizon: int = 10,
                    source=None, limit=None):
    """ARIMA baseline and Chronos-Bolt zero-shot forecasts for the same series."""
    limit = limit or settings.default_limit
    s = _load_clean(symbol, interval, source, limit)
    if s is None or s.shape[0] < MIN_OBS:
        return None

    arima = predict(symbol, interval=interval, horizon=horizon, source=source, limit=limit)
    if arima is None:
        return None

    mean, lower, upper = chronos_forecast(s, horizon)
    freq = preprocess.FREQ.get(interval, "1min")
    last_ts = s.index[-1]
    future_idx = pd.date_range(start=last_ts, periods=horizon + 1, freq=freq)[1:]
    if pd.Timestamp(arima["last_ts"]) != last_ts:
        order = arima.get("order") or [1, 1, 1]
        arima_model, used_order = fit_arima(s, order=order)
        arima_mean, arima_ci = arima_forecast(arima_model, horizon)
        arima = {
            "order": list(used_order),
            "metrics": None,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "forecast": [
                {
                    "ts": future_idx[i].isoformat(),
                    "yhat": float(arima_mean[i]),
                    "yhat_lower": float(arima_ci[i, 0]),
                    "yhat_upper": float(arima_ci[i, 1]),
                }
                for i in range(horizon)
            ],
        }

    chronos_points = [
        {
            "ts": future_idx[i].isoformat(),
            "yhat": float(mean[i]),
            "yhat_lower": float(lower[i]),
            "yhat_upper": float(upper[i]),
        }
        for i in range(horizon)
    ]

    log_return_order = [1, 0, 1]
    log_return_result = log_return_forecast(s, horizon, order=log_return_order)
    log_return_points = []
    log_return_metrics = None
    if log_return_result is not None:
        lr_mean, lr_lower, lr_upper, log_return_order = log_return_result
        log_return_metrics = log_return_backtest_holdout(s, order=log_return_order)
        log_return_points = [
            {
                "ts": future_idx[i].isoformat(),
                "yhat": float(lr_mean[i]),
                "yhat_lower": float(lr_lower[i]),
                "yhat_upper": float(lr_upper[i]),
            }
            for i in range(horizon)
        ]

    return {
        "symbol": symbol,
        "interval": interval,
        "last_ts": last_ts.isoformat(),
        "last_value": float(s.iloc[-1]),
        "horizon": horizon,
        "models": [
            {
                "model": "ARIMA",
                "order": arima.get("order"),
                "metrics": arima.get("metrics"),
                "trained_at": arima.get("trained_at"),
                "forecast": arima.get("forecast", []),
            },
            {
                "model": "Log-return ARIMA",
                "order": log_return_order,
                "metrics": log_return_metrics,
                "trained_at": datetime.now(timezone.utc).isoformat(),
                "forecast": log_return_points,
            },
            {
                "model": "Chronos-Bolt tiny",
                "forecast": chronos_points,
            },
        ],
    }


def _lookup_last_value(symbol: str, interval: str, source, limit: int, last_ts: str):
    """이전 버전 저장 모델에 last_value 가 없을 때 DB에서 같은 시점 close 를 찾는다."""
    s = _load_clean(symbol, interval, source, limit)
    if s is None or s.empty:
        return None

    target = pd.Timestamp(last_ts)
    if target in s.index:
        return float(s.loc[target])
    return None


def model_info(symbol: str, interval: str = "1m"):
    payload = store.load_model(symbol, interval)
    if payload is None:
        return None
    return {
        "symbol": payload["symbol"],
        "interval": payload["interval"],
        "order": payload["order"],
        "metrics": payload.get("metrics"),
        "trained_at": payload["trained_at"],
        "last_ts": payload["last_ts"],
        "n_obs": payload["n_obs"],
    }
