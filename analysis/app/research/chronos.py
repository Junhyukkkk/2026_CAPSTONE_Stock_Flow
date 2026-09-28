"""Chronos-Bolt(사전학습 시계열 모델) 참가자. 학습 없이(zero-shot) 과거 가격만 보고 예측한다.

분할 매매(5개 트랜치가 서로 다른 날 매매)를 위해 매일 예측한다. 날짜마다 대상 종목 전체를 한 배치로 돌린다.
t 일 예측에는 t 종가까지의 가격만 넣는다.
"""
from functools import lru_cache

import numpy as np
import pandas as pd

from .backtest import is_rebalance_day
from .features import HORIZON

MODELS = {"tiny": "amazon/chronos-bolt-tiny", "small": "amazon/chronos-bolt-small"}
CONTEXT = 512
MIN_CONTEXT = 60


@lru_cache(maxsize=2)
def _pipeline(size: str):
    import torch
    from chronos import BaseChronosPipeline
    torch.set_num_threads(2)
    return BaseChronosPipeline.from_pretrained(MODELS[size], device_map="cpu", torch_dtype=torch.float32)


def _median_forecast(size: str, series_list: list) -> np.ndarray:
    """로그 가격 시계열 목록 → HORIZON 일 뒤 예상 로그 가격(중앙값)."""
    import torch
    ctx = [torch.tensor(s[-CONTEXT:], dtype=torch.float32) for s in series_list]
    with torch.no_grad():
        q = _pipeline(size).predict(ctx, prediction_length=HORIZON)   # [batch, quantiles, horizon]
    q = q.detach().cpu().numpy()
    return q[:, q.shape[1] // 2, HORIZON - 1]


def coin_scores(close: pd.DataFrame, universe: pd.DataFrame, start, end, size: str = "tiny",
                every_day: bool = True) -> pd.Series:
    """과제 A 점수: 대상 종목의 예상 5일 로그 수익률. every_day=False 면 매매일(오프셋 0)에만."""
    logp = np.log(close)
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    out = []
    for t in close.loc[start:end].index:
        if not every_day and not is_rebalance_day(t, start):
            continue
        members = universe.columns[universe.loc[t].to_numpy()]
        hist = logp.loc[:t, members]
        names, series = [], []
        for sym in members:
            s = hist[sym].dropna().to_numpy()
            if len(s) >= MIN_CONTEXT:
                names.append(sym)
                series.append(s)
        if not series:
            continue
        pred = _median_forecast(size, series)
        last = np.array([s[-1] for s in series])
        out.append(pd.Series(pred - last, index=pd.MultiIndex.from_product([[t], names],
                                                                              names=["date", "symbol"])))
    return pd.concat(out) if out else pd.Series(dtype=float)


def market_signal(index_level: pd.Series, start, end, size: str = "tiny", every_day: bool = True) -> pd.Series:
    """과제 B 신호: 지수의 예상 5일 수익률이 0 보다 크면 1."""
    logp = np.log(index_level)
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    days = [t for t in index_level.loc[start:end].index if every_day or is_rebalance_day(t, start)]
    series = [logp.loc[:t].dropna().to_numpy() for t in days]
    preds = []
    for i in range(0, len(series), 64):
        preds.extend(_median_forecast(size, series[i:i + 64]))
    last = np.array([s[-1] for s in series])
    return pd.Series((np.array(preds) - last > 0).astype(float), index=days)
