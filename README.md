# StockFlow - 실시간 주가 수집 및 분석 시스템

실시간 주식/암호화폐 데이터 수집 → Kafka → Redis/WebSocket 실시간 화면, TimescaleDB 저장,
백테스팅, 모니터링(Prometheus/Grafana/Loki)까지 갖춘 캡스톤 프로젝트.

## 프로젝트 구조

```
2026_CAPSTONE_Stock_Flow/
├── collectors/                # 데이터 수집 계층 (Python)
│   ├── binance_producer.py    # Binance 암호화폐 데이터 수집
│   ├── alpaca_producer.py     # Alpaca 주식 데이터 수집
│   ├── config.py              # 설정 관리
│   ├── normalizer.py          # 데이터 정규화
│   ├── kafka_producer.py      # Kafka Producer 래퍼
│   ├── healthcheck.py         # 컨테이너 헬스체크
│   ├── tests/                 # 단위 테스트
│   ├── requirements.txt       # Python 의존성
│   ├── Dockerfile / docker-compose.yml
│   └── README.md              # Collector 상세 문서
│
├── backend/                   # 백엔드 (Java 17 / Spring Boot 3.4)
│   ├── stockflow-core/        # 공통 모듈 (DTO, 에러, 메트릭, 재시도)
│   ├── stockflow-realtime/    # 메인 서비스 — Consumer/저장/배치/백테스트/API/UI 전부 포함
│   │   └── src/main/resources/static/ui/   # 실시간 시세·종목·백테스트 웹 UI
│   ├── infra/                  # docker compose 인프라 전체
│   │   ├── docker-compose.yml  # TimescaleDB, Kafka, Redis, Prometheus, Grafana, Loki, Alertmanager 등
│   │   ├── create-topics.sh    # Kafka Topic 수동 생성 스크립트
│   │   └── README.md           # 인프라 상세 문서
│   ├── perf/                   # 파이프라인 부하 테스트/병목 측정 하네스
│   │   └── README.md
│   └── docs/                   # Grafana 대시보드 정의, 아키텍처/운영 메모
│
├── RUNBOOK.md                  # 운영 서버(114.71.51.41) 조작 가이드
├── TESTING_GUIDE.md             # Kafka/Consumer 동작 확인 가이드
└── README.md
```

> 참고: 시계열 예측 서비스(`stockflow-analysis`, FastAPI/ARIMA)는 이 저장소와 별도로 운영된다.

## 주요 구성 요소

### 1. 데이터 수집 계층 (Python)
- **Binance Collector**: 거래 중인 USDT 마켓 코인 전 종목 실시간 수집, 자동 재연결/백오프
- **Alpaca Collector**: IEX 거래소 주식 실시간 수집
- **데이터 정규화**: 모든 데이터를 `NormalizedTradeDTO` 형식으로 통일해 Kafka로 전송

### 2. 스트리밍 계층 (Kafka)
- **Topic**: `market.normalized`(정규화 통합, 12파티션), `market.retry`, `market.dlq`
- **Consumer**: `stockflow-realtime` 모듈에서 실시간/저장/재시도 컨슈머로 분리 소비

### 3. 실시간 처리 계층 (Java/Spring, `stockflow-realtime`)
- Kafka Consumer → Redis 캐시 → WebSocket 브로드캐스트로 클라이언트에 실시간 전송
- 실시간/저장 경로를 별도 프로세스(`--profile split`)로 분리 배포 가능

### 4. 데이터 저장 계층 (TimescaleDB)
- Flyway 마이그레이션(V1~V12)으로 스키마 관리, 하이퍼테이블/연속 집계/압축·보관 정책 적용

### 5. 배치 & 백테스팅 (`stockflow-realtime` 내 `batch`, `backtest` 패키지)
- Spring Batch 기반 일일 기술적 지표 계산
- 전략 기반 백테스트 엔진 + 웹 UI(`ui/backtest.html`)

### 6. 관측/알림 (Prometheus, Grafana, Loki, Alertmanager)
- Kafka/JVM/Redis·HikariCP/HTTP 로그/앱 커스텀 지표/Consumer 적체 대시보드 7종 (`backend/docs/*.json`)
- Consumer 적체·유실 임박 경보를 Alertmanager → Discord 웹훅으로 전송
- 로그는 Loki로 수집, Grafana에서 LogQL로 조회

### 7. 웹 UI (`stockflow-realtime` 정적 리소스)
- `ui/live.html`: 실시간 시세 화면
- `ui/stocks.html`, `ui/index.html`: 종목/시장 개요
- `ui/backtest.html`: 백테스트 실행 화면
- `storage-overview.html`: 저장 적재 현황

## 빠른 시작

### 1. 인프라 + 앱 실행

```bash
cd backend/infra
cp .env.example .env          # 대부분 기본값으로 동작, 필요 시 DISCORD_WEBHOOK 등 채움
docker compose up -d --build  # 첫 실행은 앱 이미지 빌드 때문에 몇 분 소요
```

기동되는 것 (요약, 상세는 [backend/infra/README.md](backend/infra/README.md) 참조):

| 서비스 | 주소 | 설명 |
| --- | --- | --- |
| 실시간 시세 화면 | http://localhost:8081/ui/live.html | 실시간 체결 데이터 |
| Swagger | http://localhost:8081/swagger-ui.html | REST API 문서 |
| Grafana | http://localhost:3001 (admin/admin) | 메트릭 + 로그 대시보드 |
| Prometheus | http://localhost:9090 | 메트릭 · 경보 |
| Kafka UI | http://localhost:8989 | 토픽/메시지 확인 |
| Redis Insight | http://localhost:5540 | Redis 확인 |

### 2. Kafka Topic 확인/재생성 (필요 시)

`kafka-setup` 서비스가 `docker compose up` 때마다 자동 생성하므로 보통 불필요.
수동 재생성이 필요할 때만:

```bash
cd backend/infra
./create-topics.sh
```

### 3. 데이터 수집기 (Binance는 compose에 포함되어 기본 실행됨)

```bash
cd collectors
cp env.example .env
# ALPACA_API_KEY / ALPACA_API_SECRET 설정 시 Alpaca 수집기도 사용 가능

pip install -r requirements.txt
python binance_producer.py   # 또는 docker compose 로 이미 실행 중
python alpaca_producer.py
```

### 4. 백엔드 로컬 빌드/실행 (compose 없이 개발할 때)

```bash
cd backend
./gradlew build
./gradlew :stockflow-realtime:bootRun
```

## 부하 테스트 / 성능 측정

`backend/perf`에 파이프라인 병목 측정 하네스가 있다 (초당 2천~1만 건 스윕, 개선 옵션 on/off 비교).
자세한 내용은 [backend/perf/README.md](backend/perf/README.md) 참조.

## 운영 문서

- [RUNBOOK.md](RUNBOOK.md): 운영 서버 접속, 스택 조작, Discord 경보, 장애 대응
- [TESTING_GUIDE.md](TESTING_GUIDE.md): Kafka/Consumer 동작을 직접 확인하는 방법

## 데이터 형식

모든 수집 데이터는 다음 형식으로 정규화된다:

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

## 환경 변수 설정

### Collector (Python)
`collectors/.env` 파일 생성:
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ALPACA_API_KEY=your-api-key
ALPACA_API_SECRET=your-api-secret
```

### 인프라 (`backend/infra/.env`)
```bash
DISCORD_WEBHOOK=            # Alertmanager → Discord 경보 웹훅 (선택)
SENTRY_DSN=                 # 에러 트래킹 (선택, --profile sentry)
```
전체 옵션은 [backend/infra/.env.example](backend/infra/.env.example) 참조.

### 백엔드(Java) — compose 없이 로컬 실행 시
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
DB_HOST=localhost
DB_PORT=5433
DB_NAME=stockflow
DB_USERNAME=postgres
DB_PASSWORD=postgres
REDIS_HOST=localhost
REDIS_PORT=6379
```

## 개발 가이드

- 데이터 수집기: [collectors/README.md](collectors/README.md)
- 인프라: [backend/infra/README.md](backend/infra/README.md)
- 부하 테스트: [backend/perf/README.md](backend/perf/README.md)

## 기술 스택

- **언어**: Python 3.9+, Java 17
- **프레임워크**: Spring Boot 3.4.1 (Spring Kafka, Spring Batch, Spring Data Redis, Flyway)
- **메시징**: Apache Kafka
- **데이터베이스**: TimescaleDB (PostgreSQL)
- **캐시**: Redis
- **관측**: Prometheus, Grafana, Loki, Promtail, Alertmanager, Sentry/GlitchTip
- **컨테이너**: Docker, Docker Compose

## 라이선스

캡스톤 디자인 프로젝트
