# StockFlow Analysis Service (Python · FastAPI · Forecasting)

OHLCV 시계열을 기반으로 **가격 ARIMA, 로그수익률 ARIMA, Chronos-Bolt 예측**을 제공하는 독립 서비스다.
기술적 지표(MA·RSI·MACD·볼린저·ATR·OBV 등) 계산은 Spring 배치
(`stockflow-realtime`의 `TechnicalIndicatorService`)에서 일원화하여 담당하므로,
본 서비스는 **전처리 + 예측**에만 집중한다.

## 역할 분리

```
TimescaleDB (OHLCV) ──▶ [analysis] 전처리 → ARIMA 학습/예측 → REST(JSON)
지표 계산              ──▶ Spring 배치 (Java) 에서 담당 (중복 계산 안 함)
```

데이터 소스
- `ohlcv_1m` (분봉, 실시간 집계 + 거래소 확정본) — 기본값
- `symbol_daily_ohlcv` (일봉, 배치 집계 테이블)

> 데이터 누적 기간이 짧을 때는 포인트 수가 많은 **분봉(1m)** 이 ARIMA 학습에 유리하다.

## 구조

```
analysis/
├── app/
│   ├── main.py          # FastAPI 엔드포인트
│   ├── config.py        # 환경변수 설정
│   ├── db.py            # TimescaleDB OHLCV 조회
│   ├── preprocess.py    # 결측/이상치/주기 정렬
│   ├── eval.py          # MAE/RMSE, 홀드아웃 백테스트
│   ├── service.py       # 학습/예측 오케스트레이션
│   ├── schemas.py       # 요청/응답 스키마
│   └── model/
│       ├── arima.py     # ARIMA 학습/예측 (AIC 격자 차수 선택)
│       └── store.py     # 모델 저장/로드 (joblib)
├── models/              # 학습된 모델 파일
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## 실행

### Docker (권장 — 나머지 인프라와 동일 네트워크)

`backend/infra` 스택이 떠 있어 `infra_default` 네트워크가 존재해야 한다.

```bash
cd analysis
docker compose up -d --build
# http://localhost:8000/docs (Swagger)
```

### 로컬 (Python 3.10+ 필요)

```bash
cd analysis
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp env.example .env          # 호스트 DB는 localhost:5433
uvicorn app.main:app --reload --port 8000
```

## Binance 일봉 누락 데이터 보완

일봉 예측과 백테스트가 동일한 실제 캔들 데이터를 사용하도록 Binance 일봉 누락분을 확인할 수 있다. 기본 실행은 조회만 수행하며 기존 행을 수정하지 않는다.

```bash
python -m app.scripts.backfill_binance_daily \
  --symbol BTCUSDT \
  --from 2026-05-26 \
  --to 2026-09-06
```

출력된 누락 날짜를 검토한 뒤 `--apply`를 추가하면 `(symbol, trade_date, source)` 기준으로 없는 행만 저장한다. 당일 UTC 캔들은 완료되지 않은 데이터이므로 입력할 수 없다.

```bash
python -m app.scripts.backfill_binance_daily \
  --symbol BTCUSDT \
  --from 2026-05-26 \
  --to 2026-09-06 \
  --apply
```

## API

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/health` | 상태 확인 |
| GET | `/predict/{symbol}?interval=1m&horizon=10&retrain=false` | 예측 (모델 없으면 즉석 학습) |
| GET | `/predict/{symbol}/compare?interval=1d&horizon=10` | 세 모델 예측 비교 |
| POST | `/backtest/prediction-signals` | 일봉 워크포워드 예측 신호 생성 |
| POST | `/train/{symbol}?interval=1m&limit=2000` | 학습 후 저장 |
| GET | `/models/{symbol}?interval=1m` | 저장 모델 메타데이터 |

### Spring 경유 API (`stockflow-realtime`, 8081)

화면·외부 호출은 위 FastAPI 를 직접 부르지 않고 Spring 을 거친다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/predictions/{symbol}/compare?interval=1m&horizon=10&source=` | 세 모델 비교(`/predict/{symbol}/compare` 프록시). 응답은 **비동기로 DB 에 자동 저장** |
| GET | `/api/predictions/{symbol}/history?interval=1m&limit=20` | 저장된 예측 이력, 기준 봉 시각 내림차순 (`limit` 최대 200, 기본 20) |

- 저장 테이블: `prediction_runs`(실행 헤더, `(symbol, interval, horizon, source, base_ts)` 유니크) · `prediction_forecast_points`(모델별 예측점) — Flyway V17.
- 저장은 별도 스레드·유한 큐(기본 1000)로 처리해 응답 지연에 영향이 없다. 큐가 가득 차면 버리고 카운터 `prediction.history.dropped` 가 오른다.
- `PREDICTION_HISTORY_ENABLED=false` 면 저장을 끄고 `history` 는 빈 배열을 돌려준다(기본 true).
- 학습에 필요한 최소 관측치는 **50개**(`app/service.py` `MIN_OBS`)다. 시뮬레이터(`source=SIMULATOR`) 종목은 켠 뒤 1분봉이 50개 쌓이는 약 1시간 뒤부터 예측이 나온다.
- 부하 중 응답: 첫 호출(모델 학습) 10~15초, 이후 중앙값 0.7~1.1초 (파이프라인 4,000~20,000/s 부하, [OPTIMIZATION_HISTORY.md §4](../backend/perf/OPTIMIZATION_HISTORY.md)).
- 서버에서는 `backend/infra/reserve-analysis-capacity.sh` 로 이 컨테이너에 코어 6–7·메모리 3GB 를 예약한다(컨테이너 재생성 후 재실행, [RUNBOOK.md](../RUNBOOK.md)).

예시

```bash
curl -X POST "http://localhost:8000/train/BTCUSDT?interval=1m"
curl "http://localhost:8000/predict/BTCUSDT?interval=1m&horizon=15"
```

예측 응답(요약)

```json
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "order": [2, 1, 2],
  "metrics": { "mae": 12.3, "rmse": 18.7, "test_size": 50, "order": [2,1,2] },
  "trained_at": "2026-06-02T...Z",
  "last_ts": "2026-06-02T...Z",
  "horizon": 15,
  "forecast": [
    { "ts": "...", "yhat": 67250.1, "yhat_lower": 67100.0, "yhat_upper": 67400.2 }
  ]
}
```

## 비고
- 차수 (p,d,q) 는 작은 격자에서 AIC 최소를 선택한다. 추후 `pmdarima.auto_arima` 로 교체 가능.
- 예측 비교 UI와 Spring 백테스트 연동은 `stockflow-realtime`에서 제공한다.
- Spring Batch 재학습 트리거 연동은 다음 단계다.
