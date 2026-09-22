"""API 요청/응답 스키마 (pydantic)."""
from datetime import date
from typing import List, Literal, Optional

from pydantic import BaseModel, Field


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


class PredictionSignalRequest(BaseModel):
    symbol: str
    model: Literal["ARIMA", "LOG_RETURN_ARIMA", "CHRONOS_BOLT"]
    from_date: date
    to_date: date
    source: Optional[str] = "BINANCE"
    warmup: int = Field(default=50, ge=50, le=500)
    refit_every: int = Field(default=5, ge=1, le=30)
    max_history: int = Field(default=200, ge=50, le=2000)
    volatility_window: int = Field(default=20, ge=5, le=100)
    volatility_multiplier: float = Field(default=0.5, ge=0, le=5)
    fee_bps: float = Field(default=10, ge=0, le=1000)
    slippage_bps: float = Field(default=5, ge=0, le=1000)


class PredictionSignalPoint(BaseModel):
    signal_date: date
    execution_date: date
    reference_price: float
    predicted_price: float
    expected_return_pct: float
    threshold_pct: float
    signal: Literal["BUY", "HOLD", "SELL"]


class PredictionSignalResponse(BaseModel):
    symbol: str
    model: str
    from_date: date
    to_date: date
    warmup: int
    refit_every: int
    fee_bps: float
    slippage_bps: float
    signal_count: int
    buy_count: int
    hold_count: int
    sell_count: int
    mae: float
    rmse: float
    mae_pct: float
    rmse_pct: float
    signals: List[PredictionSignalPoint]
