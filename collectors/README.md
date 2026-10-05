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
모든 메시지는 `source="SIMULATOR"`(`SIM_SOURCE_LABEL` 로 변경 가능), `exchange="SIM"`, `marketType="STOCK"` 이다. `tradeId` 는
`SIM-{symbol}-{run_id}-{seq}` 이며 재시작해도 겹치지 않는다. 라벨을 바꾸면 접두어도 `{라벨}-{symbol}-{run_id}-{seq}` 가 된다.

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

### 발생률 모드 (`SIM_RATE_MODE`)
- **`realistic`(코드 기본값)** — 종목별 하루 평균 통합 체결 건수 `daily_trades`(`universe.csv` 선택 컬럼, 근사치)로 발생률을 정한다.
  종목 i 의 순간 발생률 = `daily_trades_i / 23400 × SIM_RATE_SCALE × profile(t)` (23400 = 정규장 6.5시간 초, 도착은 구간별 포아송).
  `SIM_TOTAL_TPS` 는 쓰이지 않으며 시작 로그에 "SIM_TOTAL_TPS 무시됨"과 계산된 합계 평균 TPS 가 출력된다.
  - 번들 105종목 합계 약 **1,948만 건/일 → 평균 약 832 TPS** (`SIM_RATE_SCALE=1`). 종목 하나가 하루 수십만~백만+ 건
    (NVDA 150만, TSLA 130만, SPY 120만, QQQ 90만, AAPL 90만 …)이고 중형·방어주는 4만~12만 건이다.
  - `SIM_MARKET_HOURS=us`: 정규장 안에서 U자 강도 곡선 `profile(t)` 를 곱한다 (개장 직후 약 2.9배 → 30분 뒤 약 1.4배 → 정오 약 0.6배 →
    마감 직전 약 2.4배, 꼭짓점 사이 선형 보간, 장 전체 평균이 정확히 1.0 이 되도록 정규화). 장외·주말은 0.
  - `SIM_MARKET_HOURS=always`: 곡선 없이 평균(profile=1)으로 24시간 일정하게 만든다(한국 시간 낮에도 데이터가 나온다).
    **같은 `SIM_RATE_SCALE` 이라도 24시간 흐르므로 `us` 모드(약 6.5시간)의 약 3.7배 건수**(scale 1 ≈ 7,190만 건/일)다.
  - `daily_trades` 가 비었거나 숫자가 아닌/0 이하인 행은 그 값만 무시하고 `weight × 10,000` 건/일로 폴백한다(컬럼이 없는 구형 CSV 도 동작).
- **`fixed`** — 기존 동작. `SIM_TOTAL_TPS` 를 `weight` 비례로 나눠 항상 일정하게 낸다(강도 곡선·`daily_trades` 미사용).

체결 간 변동성은 평균 도착 간격으로 환산되므로 체결이 많은 종목일수록 틱당 변동폭이 작고, `SIM_RATE_SCALE` 을 바꿔도
하루 합산 변동성은 종목 `volatility` 수준(일간 수익률 표준편차 ≈ `volatility/√252`)으로 유지된다. realistic + `always` 는 24시간 기준으로
환산하므로 (fixed + always 와 달리) 일 변동성이 2배로 부풀지 않는다. 장중 변동성은 거래 강도에 비례해 개장·마감 직후 커진다.

### 운영 부하 (용량 안내)
> ⚠️ **realistic · scale 1.0 은 DB 를 빠르게 키운다.** 105종목 합계 평균 약 **832 TPS**(하루 약 2,000만 건, `always` 면 약 7,000만 건)다.
> 체결마다 DB 행이 하나씩 생겨 압축 전 기준 DB 가 하루 수 GB 씩 늘고, 단일 HDD 서버에서는 대량 삭제·집계 갱신이 iowait 를 만든다
> ([RUNBOOK.md §8](../RUNBOOK.md)). Redis 멱등성 키는 기본 TTL 10분(`STOCKFLOW_IDEMPOTENCY_TTL_SECONDS`)이라 누적량이 10분치로 제한된다(옛 기본값 86400초로 되돌리면 2GB Redis 가 가득 차 키가 축출된다).
> 용량 한계: 앱은 시뮬레이터 부하 약 12,000/s 까지 적체 없이 소화한다([OPTIMIZATION_HISTORY.md](../backend/perf/OPTIMIZATION_HISTORY.md), 2026-10-05 코드·단일 서버).
> 장기 가동 시 `SIM_RATE_SCALE` 로 DB 증가량을 정한다.

compose(`backend/infra`·`collectors`)의 기본값은 `SIM_RATE_MODE=realistic`, **`SIM_RATE_SCALE=0.25`**(서버 보호용 기본값, 합계 평균 약 208 TPS)이다.
`SIM_TOTAL_TPS`(compose 기본 `100`, 코드 기본 `300`)는 `fixed` 모드 전용이다. 기본 `SIM_MARKET_HOURS=always` + scale 0.25 이면 하루 약 1,800만 건이므로
라이브 스택에서는 `SIM_RATE_SCALE` 을 더 낮추거나 `SIM_MARKET_HOURS=us` 를 권장한다.
(현재 학교 서버의 라이브 시뮬레이터는 의도적으로 `SIM_RATE_SCALE=1.0`·`always` 로 운영한다. 디스크 여유는 3.2TB 이고 압축 전 증가량을 모니터링한다. 줄이려면 [RUNBOOK.md §8](../RUNBOOK.md#8-시뮬레이터--테스트-데이터)의 `docker run` 절차로 낮춘 값을 주고 컨테이너를 다시 띄운다.)

`fixed` + `always` 는 변동성을 거래 초당 기준으로 스케일하면서 24시간 내내 돌기 때문에 일봉 변동성이 현실의 약 2배다(`realistic` 은 이 한계가 없다).
시뮬레이션 데이터의 지표·백테스트는 의미가 없다.

> ⚠️ **높은 `SIM_RATE_SCALE`(약 30 이상, 즉 약 2.5만 msg/s 이상)에서는 단일 프로세스 한계에 걸릴 수 있다.**
> 체결 생성 자체는 가볍다(scale 100 에서 50ms 스텝당 CPU 약 19ms). 병목은 메시지마다 거치는 Kafka `produce()` 경로(단일 프로세스·단일 프로듀서)다.
> 따라서 실제 전송 속도가 목표에 못 미치면 루프가 밀렸다가 몰아서 보내며, 겉으로는 에러 없이 종료 통계의 `큐 거부` 건수로만 드러난다.
> - 시작 로그의 **"합계 평균 TPS"**(기대 속도)와 실제 속도를 비교한다. 종료 통계 줄(`📊 최종 통계`)의 **`전송`**(전송 성공 건수)·`큐 거부`(프로듀서 큐가 받지 않은 건수)·`평균 속도`로 확인한다.
> - 종료 시 `전송` 이 `기대 TPS × 활성 시간` 의 90% 미만이면(활성 10초 이상, dry-run 제외) 시뮬레이터가 **WARNING** 으로 미달 폭을 알려 준다.
>   (`SIM_MARKET_HOURS=us` + realistic 은 강도 곡선 때문에 짧은 구간 평균이 기대값과 달라 이 경고를 내지 않는다.)
> - 한 프로세스 용량을 넘는 속도가 필요하면 **시뮬레이터 프로세스를 여러 개** 띄우고 `SIM_RATE_SCALE` 을 나눠 준다
>   (예: 합계 scale 60 → 프로세스 3개 × 20). `SIM_SOURCE_LABEL` 은 프로세스마다 달리하거나 같게 해도 된다 —
>   `tradeId` 의 run_id 가 기동마다 달라 겹치지 않으며, 같은 라벨이면 `source` 한 번으로 한꺼번에 정리할 수 있다.

### 분석(예측) 연동
예측 API 는 `(symbol, source)` 마다 1분봉이 50개 이상(`analysis/app/service.py` `MIN_OBS`) 있어야 동작한다.
`SIM_MARKET_HOURS=us` 면 장외·주말에는 봉이 쌓이지 않으므로, 켠 뒤 약 1시간(`always` 기준) 지나야 예측이 나온다.
모델 캐시가 없는 종목의 첫 호출은 학습 때문에 10~15초, 이후 약 1초([OPTIMIZATION_HISTORY.md §4](../backend/perf/OPTIMIZATION_HISTORY.md)).

### 부하 테스트 데이터 정리
`SIM_SOURCE_LABEL=SIMLOAD` 로 만든 테스트 데이터는 끝난 뒤 지운다. 순서와 SQL 은 [RUNBOOK.md §8](../RUNBOOK.md).

### 실행
기본 `docker compose up` 에는 포함되지 않고 `sim` 프로파일에서만 기동한다.

```bash
# backend/infra 또는 collectors 디렉터리에서
docker compose --profile sim up -d stock-simulator

# 로컬 dry-run: Kafka 없이 stdout 에 JSON 한 줄씩 출력 (로그는 stderr)
SIM_DRY_RUN=true SIM_SEED=1 SIM_RATE_MODE=realistic SIM_RATE_SCALE=0.1 python stock_simulator.py
# 고정 발생률: SIM_RATE_MODE=fixed SIM_TOTAL_TPS=50
```

### 주요 환경 변수

| 변수 | 기본값 | 설명 |
|---|---|---|
| `SIM_RATE_MODE` | `realistic` | `realistic`(종목별 `daily_trades` 기반 실제 체결량, `us` 에서 U자 강도 곡선) 또는 `fixed`(기존 `SIM_TOTAL_TPS`) |
| `SIM_RATE_SCALE` | `1.0` (compose 는 `0.25`) | realistic 발생률 배율 (0 초과 **100 이하**) — 평소엔 서버 용량에 맞춰 줄이고, 처리 용량 한계 측정에서는 키운다(scale 12 ≈ 1만 TPS) |
| `SIM_SOURCE_LABEL` | `SIMULATOR` (compose 도 동일) | 출력 `source` 값과 tradeId 접두어. `^[A-Z0-9_]{1,32}$`. 부하 테스트는 `SIMLOAD` 처럼 따로 지정해 운영 데이터와 분리하고 나중에 `source` 로 삭제한다. 기본 라벨의 tradeId 접두어는 기존대로 `SIM`. `exchange=SIM`·`marketType=STOCK` 은 라벨과 무관하게 유지 |
| `SIM_TOTAL_TPS` | `300` (compose 는 `100`) | **`fixed` 모드 전용** 전 종목 합계 초당 체결 수 (종목별 빈도는 `weight` 비례, 포아송 도착) |
| `SIM_MARKET_HOURS` | `always` | `always` 또는 `us` (`us` = 미국 동부시간 월~금 09:30~16:00 에만 전송, 휴장일 미반영) |
| `SIM_PRICE_SOURCE` | `static` | 시작가 출처. `static`=CSV 그대로, `alpaca`=Alpaca 스냅샷의 최신 체결가, `auto`=키가 있으면 alpaca 시도 |
| `SIM_SEED` | (없음) | 지정하면 같은 가격·수량 시퀀스 재현 (`tradeId` 의 run_id 는 기동마다 다름) |
| `SIM_DRY_RUN` | `false` | `true` 면 Kafka 대신 stdout 에 출력 |
| `SIM_TICK_INTERVAL_MS` | `50` | 체결 생성 스케줄러 간격 (1~2000) |
| `SIM_SYMBOLS_FILE` | `simulator/universe.csv` | 종목 CSV (`symbol,price,volatility,weight[,daily_trades]`) |
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
  "source": "BINANCE" | "ALPACA" | "SIMULATOR",
  "symbol": "BTCUSDT" | "AAPL",
  "price": "50000.12345678",
  "volume": "1.5",
  "tradeId": "unique-trade-id",
  "exchange": "BINANCE" | "IEX" | "SIM",
  "timestamp": 1234567890123,
  "receivedAt": 1234567890124,
  "marketType": "CRYPTO" | "STOCK"
}
```

## Kafka Topics

- `market.normalized`: Binance/Alpaca 공통 정규화 시세 (12 파티션, 4시간 retention). Consumer가 구독하는 유일한 시세 토픽.
- `market.retry`: 재시도 대기 메시지 (6 파티션, 4시간 retention)
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
pip install -r requirements.txt -r requirements-dev.txt
pytest tests/        # 설정·정규화·시뮬레이터(universe/generator/realistic/price_seed/runner)
```
