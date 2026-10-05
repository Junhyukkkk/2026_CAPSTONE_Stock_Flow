# StockFlow Data Collectors

실시간 주식/암호화폐 데이터 수집기 (Binance + Alpaca)

## 구조

```
collectors/
├── binance_producer.py    # Binance 암호화폐 데이터 수집
├── alpaca_producer.py     # Alpaca 주식 데이터 수집
├── stock_simulator.py     # 미국 주식 시세 시뮬레이터 (가짜 체결 생성)
├── simulator/             # 시뮬레이터 모듈 (종목 CSV, 가격 모델, 시작가 로더)
├── config.py              # 설정 관리
├── normalizer.py          # 데이터 정규화 (NormalizedTradeDTO)
├── kafka_producer.py      # Kafka Producer 래퍼
├── utils.py               # 유틸리티 함수
├── requirements.txt       # Python 의존성
├── Dockerfile             # Docker 이미지 빌드
├── docker-compose.yml     # Docker Compose 설정
└── README.md              
```

## 기능

### Binance Collector
- 거래 중인 USDT 마켓 전 종목 실시간 수집 (`BINANCE_TOP_SYMBOLS_LIMIT`으로 상위 거래량 N개만 수집하도록 제한 가능)
- 자동 재연결 및 백오프 전략
- 종목 리스트 주기적 갱신
- Kafka로 정규화된 데이터 전송

### Alpaca Collector
- IEX 거래소 전체 종목 실시간 수집
- Trade(체결) 및 Quote(호가) 데이터 수집
- Kafka로 정규화된 데이터 전송

### 공통 기능
- **정규화**: 모든 데이터를 `NormalizedTradeDTO` 형식으로 변환
- **에러 처리**: 강력한 예외 처리 및 재시도 로직
- **메트릭**: 전송 통계, 성공률, 속도 모니터링
- **설정 외부화**: 환경 변수 기반 설정 관리

## 주식 시세 시뮬레이터

> ⚠️ **가짜 데이터 경고** — 시뮬레이터가 만드는 체결은 전부 시뮬레이션이다. 실제 시세가 아니다.
> 이 데이터로 얻은 분석·백테스트·수익률 결과는 실제 시장에 대한 결론으로 해석하면 안 된다.
> (시작가만 선택적으로 Alpaca 실제 시세를 읽어 쓰며, 이후의 가격 움직임·수량·체결 시각은 모두 무작위 생성이다.)

### 목적
미국 주식 전 종목 실시간 수집은 Alpaca 유료 플랜이 필요해 예산상 어렵다. 이를 대체해, 대형주·ETF 약 100개가
실제 주식처럼(종목별 유동성에 비례한 체결 빈도, 변동성 있는 가격) 움직이는 체결을 Kafka 로 쏴 준다.
기존 파이프라인(`market.normalized` → Redis/WebSocket/TimescaleDB)은 코드 변경 없이 STOCK 으로 처리한다.

### 구분 방법
모든 메시지는 `source="SIMULATOR"`, `exchange="SIM"`, `marketType="STOCK"` 이다. `tradeId` 는
`SIM-{symbol}-{run_id}-{seq}` 이며 재시작해도 겹치지 않는다.

> ⚠️ **`alpaca-collector`(또는 다른 실제 시세 수집기)와 같은 종목에 동시에 돌리지 말 것.**
> `market_ticks`·`ohlcv_1m`·`symbol_daily_ohlcv` 는 `source` 별로 행이 분리되지만, 그 밖의 경로는 출처를 구분하지 못한다.
> - Redis `price:latest:{symbol}` 와 pub-sub `price:{symbol}` 은 마지막에 쓴 쪽이 이긴다 (UI 에서 실제가·가짜가가 번갈아 보인다).
> - `instruments.exchange` 가 IEX 와 SIM 사이를 오간다.
> - `symbol_daily_indicators` 는 `source` 컬럼이 없어 심볼·날짜당 한 행이다 (실제일·가짜일이 이어 붙는다).
> - 일봉·분봉 히스토리 API 와 전일 종가 동기화는 출처를 합치거나 임의로 하나를 고를 수 있다.
>
> 운영 체크리스트:
> 1. 먼저 `alpaca-collector` 를 중지한다.
> 2. 시뮬레이터 종목에 기존 ALPACA 행이 있는지 확인한다: `SELECT DISTINCT symbol FROM symbol_daily_ohlcv WHERE source='ALPACA';`
> 3. `SIM_PRICE_SOURCE` 는 재시작 사이에 바꾸지 않는다 (static ↔ alpaca 전환 시 가격이 0.2~5배 점프할 수 있다).

### 운영 부하
체결마다 Redis 멱등성 키(현재 코드 기본 TTL 24시간)와 DB 행이 하나씩 생긴다. 300 ticks/s 로 24시간 돌리면 하루 약 2,600만 건이라
압축 전 기준 하루 수 GB 의 Redis 키·디스크가 필요하다. 라이브 스택에서는 낮은 TPS 와/또는 `SIM_MARKET_HOURS=us` 를 권장한다.
이 때문에 **compose 의 `SIM_TOTAL_TPS` 기본값은 `100`** 이고(`backend/infra`·`collectors` 두 compose 모두), 환경 변수 없이
`python stock_simulator.py` 로 직접 실행할 때의 코드 기본값만 `300` 이다.

기본 `always` 모드는 변동성을 거래 초당 기준으로 스케일하면서 24시간 내내 돌기 때문에 일봉 변동성이 현실의 약 2배다.
시뮬레이션 데이터의 지표·백테스트는 의미가 없다.

### 실행
기본 `docker compose up` 에는 포함되지 않고 `sim` 프로파일에서만 기동한다.

```bash
# backend/infra 또는 collectors 디렉터리에서
docker compose --profile sim up -d stock-simulator

# 로컬 dry-run: Kafka 없이 stdout 에 JSON 한 줄씩 출력 (로그는 stderr)
SIM_DRY_RUN=true SIM_SEED=1 SIM_TOTAL_TPS=50 python stock_simulator.py
```

### 주요 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `SIM_TOTAL_TPS` | `300` (compose 는 `100`) | 전 종목 합계 초당 체결 수 (종목별 빈도는 `weight` 비례, 포아송 도착) |
| `SIM_MARKET_HOURS` | `always` | `always` 또는 `us` (`us` = 미국 동부시간 월~금 09:30~16:00 에만 전송, 휴장일 미반영) |
| `SIM_PRICE_SOURCE` | `static` | 시작가 출처. `static`=CSV 그대로, `alpaca`=Alpaca 스냅샷의 최신 체결가, `auto`=키가 있으면 alpaca 시도 |
| `SIM_SEED` | (없음) | 지정하면 같은 가격·수량 시퀀스 재현 (`tradeId` 의 run_id 는 기동마다 다름) |
| `SIM_DRY_RUN` | `false` | `true` 면 Kafka 대신 stdout 에 출력 |
| `SIM_TICK_INTERVAL_MS` | `50` | 체결 생성 스케줄러 간격 (1~2000) |
| `SIM_SYMBOLS_FILE` | `simulator/universe.csv` | 종목 CSV (`symbol,price,volatility,weight`) |
| `ALPACA_API_KEY` / `ALPACA_API_SECRET` | (없음) | 시작가 읽기용(선택). 체결 전송에는 쓰이지 않는다 |

### 시작가 로더 (`SIM_PRICE_SOURCE`)
`alpaca`/`auto` 는 `GET https://data.alpaca.markets/v2/stocks/snapshots?feed=iex` (타임아웃 10초, 50종목씩) 로
`latestTrade.p` → `dailyBar.c` → `prevDailyBar.c` 순으로 시작가를 읽는다. 아래 경우는 해당 종목(또는 전체)이
예외 없이 CSV 가격으로 폴백한다: 키 없음(`auto` 는 요청 자체를 하지 않음), 네트워크 오류·타임아웃, 401/403(즉시 중단),
5xx·빈 응답·예상 밖 JSON, 응답에 없는 종목, 0 이하이거나 CSV 가격의 0.2~5배 밖인 가격.

## 설정

### 환경 변수

`.env` 파일을 생성하거나 환경 변수로 설정:

```bash
# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC_NAME=market.normalized

# Binance 설정 (0 이하 = 전체 종목)
BINANCE_TOP_SYMBOLS_LIMIT=0

# Alpaca 설정 (필수)
ALPACA_API_KEY=your-api-key
ALPACA_API_SECRET=your-api-secret
ALPACA_SUBSCRIBE_ALL_STOCKS=true

# 로깅
LOG_LEVEL=INFO
```

전체 설정 옵션은 `config.py` 참조

## 실행 방법

### 로컬 실행

```bash
# 의존성 설치
pip install -r requirements.txt

# Binance Collector 실행
python binance_producer.py

# Alpaca Collector 실행
python alpaca_producer.py
```

### Docker Compose 실행

```bash
# 전체 인프라 실행 (Kafka 포함)
cd ../backend/infra
docker-compose up -d

# Collectors 실행
cd ../../collectors
docker-compose up -d
```

## 데이터 형식

모든 데이터는 `NormalizedTradeDTO` 형식으로 Kafka에 전송됩니다:

```json
{
  "source": "BINANCE" | "ALPACA",
  "symbol": "BTCUSDT" | "AAPL",
  "price": "50000.12345678",
  "volume": "1.5",
  "tradeId": "unique-trade-id",
  "exchange": "BINANCE" | "IEX",
  "timestamp": 1234567890123,
  "receivedAt": 1234567890124,
  "marketType": "CRYPTO" | "STOCK"
}
```

## Kafka Topics

- `market.normalized`: Binance/Alpaca 공통 정규화 시세 (12 파티션, 4시간 retention). Consumer가 구독하는 유일한 시세 토픽.
- `market.dlq`: Dead Letter Queue - 실패한 메시지 (3 파티션, 7일 retention)

토픽은 `backend/infra/docker-compose.yml`의 `kafka-setup` 서비스가 기동 시 자동 생성한다.

**Topic 설계 상세**: [KAFKA_TOPIC_DESIGN.md](KAFKA_TOPIC_DESIGN.md) 참조

## 모니터링

각 Collector는 다음 메트릭을 제공합니다:
- 전송된 메시지 수
- 실패한 메시지 수
- DLQ로 전송된 메시지 수 (실패 메시지 자동 전송)
- 초당 메시지 수 (msg/s)
- 성공률

통계는 주기적으로 로그에 출력됩니다.

### DLQ (Dead Letter Queue)
- 실패한 메시지는 자동으로 `market.dlq` 토픽으로 전송됩니다
- DLQ 메시지 구조: 원본 메시지 + 에러 정보 + 타임스탬프
- `DLQ_ENABLED=false`로 설정하여 비활성화 가능

## 문제 해결

### Kafka 연결 실패
- Kafka가 실행 중인지 확인: `docker ps | grep kafka`
- `KAFKA_BOOTSTRAP_SERVERS` 설정 확인

### Alpaca 인증 실패
- `ALPACA_API_KEY`와 `ALPACA_API_SECRET` 확인
- API 키가 활성화되어 있는지 확인

### 데이터가 전송되지 않음
- 로그 레벨을 `DEBUG`로 변경하여 상세 로그 확인
- Kafka Topic이 생성되어 있는지 확인

## 개발 가이드

### 새로운 거래소 추가

1. `normalizer.py`에 정규화 함수 추가
2. 새로운 producer 파일 생성 (예: `coinbase_producer.py`)
3. `config.py`에 설정 추가
4. `docker-compose.yml`에 서비스 추가

### 테스트

```bash
# 단위 테스트 (추후 추가 예정)
pytest tests/

# 통합 테스트
python -m pytest tests/integration/
```
