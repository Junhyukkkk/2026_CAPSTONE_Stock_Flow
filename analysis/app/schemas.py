"""API 요청/응답 스키마 (pydantic)."""
from typing import List, Optional

from pydantic import BaseModel


class ForecastPoint(BaseModel):
    ts: str
    yhat: float
    yhat_lower: float
    yhat_upper: float


class Metrics(BaseModel):
    mae: Optional[float] = None
    rmse: Optional[float] = None
    test_size: Optional[int] = None
    order: Optional[List[int]] = None


class PredictResponse(BaseModel):
    symbol: str
    interval: str
    order: List[int]
    metrics: Optional[Metrics] = None
    trained_at: str
    last_ts: str
    last_value: Optional[float] = None
    horizon: int
    forecast: List[ForecastPoint]


class ModelForecast(BaseModel):
    model: str
    order: Optional[List[int]] = None
    metrics: Optional[Metrics] = None
    trained_at: Optional[str] = None
    forecast: List[ForecastPoint]


class ComparePredictResponse(BaseModel):
    symbol: str
    interval: str
    last_ts: str
    last_value: Optional[float] = None
    horizon: int
    models: List[ModelForecast]


class TrainResponse(BaseModel):
    symbol: str
    interval: str
    order: List[int]
    metrics: Optional[Metrics] = None
    trained_at: str
    last_ts: str
    n_obs: int


class ModelInfo(BaseModel):
    symbol: str
    interval: str
    order: List[int]
    metrics: Optional[Metrics] = None
    trained_at: str
    last_ts: str
    n_obs: int
