"""StockFlow 분석/예측 서비스 진입점 (FastAPI).

엔드포인트
- GET  /health                  서비스 상태
- GET  /predict/{symbol}        ARIMA 예측 (저장 모델 없으면 즉석 학습)
- POST /train/{symbol}          모델 학습 후 저장
- GET  /models/{symbol}         저장된 모델 메타데이터
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from . import service
from .config import settings
from .schemas import (
    ComparePredictResponse,
    IntradayPredictionSignalRequest,
    IntradayPredictionSignalResponse,
    ModelInfo,
    PredictionSignalRequest,
    PredictionSignalResponse,
    PredictResponse,
    TrainResponse,
)

app = FastAPI(
    title="StockFlow Analysis Service",
    description="OHLCV 기반 ARIMA 시계열 예측 (Python). 지표 계산은 Spring 배치에서 담당.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


@app.get("/health")
def health():
    return {"status": "ok", "default_interval": settings.default_interval}


@app.get("/predict/{symbol}", response_model=PredictResponse)
def predict(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|1d)$"),
    horizon: int = Query(default=10, ge=1, le=500),
    source: str | None = Query(default=None),
    retrain: bool = Query(default=False),
):
    result = service.predict(
        symbol.upper(), interval=interval, horizon=horizon,
        source=source, retrain=retrain,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}' 예측 불가: 학습에 필요한 데이터가 부족합니다.",
        )
    return result


@app.get("/predict/{symbol}/compare", response_model=ComparePredictResponse)
def predict_compare(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|1d)$"),
    horizon: int = Query(default=10, ge=1, le=100),
    source: str | None = Query(default=None),
):
    result = service.predict_compare(
        symbol.upper(), interval=interval, horizon=horizon, source=source,
    )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}' 비교 예측 불가: 필요한 데이터가 부족합니다.",
        )
    return result


@app.post("/train/{symbol}", response_model=TrainResponse)
def train(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|1d)$"),
    source: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=50),
):
    payload = service.train(symbol.upper(), interval=interval, source=source, limit=limit)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{symbol}' 학습 불가: 데이터가 부족합니다.",
        )
    return payload


@app.get("/models/{symbol}", response_model=ModelInfo)
def model_info(
    symbol: str,
    interval: str = Query(default="1m", pattern="^(1m|1d)$"),
):
    info = service.model_info(symbol.upper(), interval=interval)
    if info is None:
        raise HTTPException(status_code=404, detail="저장된 모델이 없습니다.")
    return info


@app.post("/backtest/prediction-signals", response_model=PredictionSignalResponse)
def create_prediction_signals(request: PredictionSignalRequest):
    try:
        result = service.prediction_signals(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{request.symbol}' 일봉 데이터를 찾을 수 없습니다.",
        )
    return result


@app.post(
    "/backtest/intraday-prediction-signals",
    response_model=IntradayPredictionSignalResponse,
)
def create_intraday_prediction_signals(request: IntradayPredictionSignalRequest):
    try:
        result = service.intraday_prediction_signals(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"'{request.symbol}' 1분봉 데이터를 찾을 수 없습니다.",
        )
    return result
