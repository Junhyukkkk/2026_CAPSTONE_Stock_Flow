"""Chronos-Bolt zero-shot forecasting wrapper.

The pipeline is loaded lazily because model startup is much heavier than ARIMA.
"""
from functools import lru_cache

import numpy as np
import torch
from chronos import BaseChronosPipeline


MODEL_ID = "amazon/chronos-bolt-tiny"


@lru_cache(maxsize=1)
def _pipeline():
    return BaseChronosPipeline.from_pretrained(
        MODEL_ID,
        device_map="cpu",
        torch_dtype=torch.float32,
    )


def forecast(series, horizon: int):
    """Return median forecast plus 0.1/0.9 quantile interval."""
    values = np.asarray(series, dtype=np.float32)
    context = torch.tensor(values[-512:], dtype=torch.float32)
    with torch.no_grad():
        pred = _pipeline().predict(context, prediction_length=horizon)

    arr = pred.detach().cpu().numpy()
    if arr.ndim != 3:
        raise RuntimeError(f"Unexpected Chronos forecast shape: {arr.shape}")

    quantiles = arr[0]
    lower = quantiles[0]
    median = quantiles[quantiles.shape[0] // 2]
    upper = quantiles[-1]
    return median.astype(float), lower.astype(float), upper.astype(float)
