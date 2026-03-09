# StockFlow - 실시간 주가 수집 및 분석 시스템

실시간 주식/암호화폐 데이터 수집과 시계열 데이터베이스를 활용한 대용량 주가 분석, 예측 및 백테스팅 서비스

## 프로젝트 구조

```
capstone/
├── collectors/          # 데이터 수집 계층 (Python)
│   ├── binance_producer.py    # Binance 암호화폐 데이터 수집
│   ├── alpaca_producer.py     # Alpaca 주식 데이터 수집
│   ├── config.py              # 설정 관리
│   ├── normalizer.py          # 데이터 정규화
│   ├── kafka_producer.py      # Kafka Producer 래퍼
│   ├── utils.py               # 유틸리티 함수
│   ├── requirements.txt       # Python 의존성
│   ├── Dockerfile             # Docker 이미지
│   ├── docker-compose.yml     # Collector 실행 설정
│   └── README.md              # Collector 상세 문서
│
├── backend/                   # 백엔드 서비스 (Java/Spring Boot)
│   ├── stockflow-api/         # REST API 서버
│   ├── stockflow-realtime/    # 실시간 처리 서비스 (Kafka Consumer, Redis, WebSocket)
│   ├── stockflow-batch/       # 배치 처리 서비스 (Spring Batch)
│   ├── stockflow-core/        # 공통 모듈 (DTO 등)
│   └── infra/                 # 인프라 설정
│       ├── docker-compose.yml # 전체 인프라 (TimescaleDB, Kafka, Redis 등)
│       └── create-topics.sh   # Kafka Topic 생성 스크립트
│
└── README.md                  
```

## 주요 구성 요소

### 1. 데이터 수집 계층 (Python)
- **Binance Collector**: 상위 거래량 300개 암호화폐 실시간 수집
- **Alpaca Collector**: IEX 거래소 전체 주식 실시간 수집
- **데이터 정규화**: 모든 데이터를 `NormalizedTradeDTO` 형식으로 통일
- **Kafka 전송**: 정규화된 데이터를 Kafka Topic으로 전송

### 2. 스트리밍 계층 (Kafka)
- **Topic**: `market.binance.tick`, `market.alpaca.tick`
- **Consumer**: `stockflow-realtime` 모듈에서 처리

### 3. 실시간 처리 계층 (Java/Spring)
- **Kafka Consumer**: 수집된 데이터 소비
- **Redis**: 실시간 캐싱 및 Pub/Sub
- **WebSocket**: 클라이언트에 실시간 데이터 전송

### 4. 데이터 저장 계층 (TimescaleDB)
- 시계열 데이터 최적화
- 시간 기반 파티셔닝

### 5. 배치 처리 계층 (Spring Batch)
- 일일 기술적 지표 계산
- 데이터 정합성 검증

### 6. 분석 & 예측 계층 (Python/FastAPI)
- ARIMA 시계열 예측 모델
- 기술적 지표 계산 (MA, 볼린저밴드 등)

## 빠른 시작

### 1. 인프라 실행

```bash
cd backend/infra
docker-compose up -d
```

이 명령으로 다음 서비스가 실행됩니다:
- TimescaleDB (포트 5432)
- Kafka + Zookeeper (포트 9092)
- Redis (포트 6379)
- Kafka UI (포트 8989)
- Redis Insight (포트 5540)

### 2. Kafka Topic 생성

```bash
cd backend/infra
./create-topics.sh
```

### 3. 데이터 수집기 실행

```bash
cd collector-python

# 환경 변수 설정 (.env 파일 생성)
cp env.example .env
# .env 파일 편집하여 ALPACA_API_KEY, ALPACA_API_SECRET 설정

# Docker Compose로 실행
docker-compose up -d

# 또는 로컬 실행
pip install -r requirements.txt
python binance_producer.py  # 별도 터미널
python alpaca_producer.py  # 별도 터미널
```

### 4. 백엔드 서비스 실행

```bash
cd backend

# Gradle로 빌드
./gradlew build

# 각 서비스 실행
./gradlew :stockflow-api:bootRun
./gradlew :stockflow-realtime:bootRun
./gradlew :stockflow-batch:bootRun
```

## 환경 변수 설정

### Collector (Python)
`collectors/.env` 파일 생성:
```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:9093
ALPACA_API_KEY=your-api-key
ALPACA_API_SECRET=your-api-secret
```

### Backend (Java)
`backend/.env` 파일 생성:
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=stockflow
DB_USERNAME=postgres
DB_PASSWORD=postgres
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_HOST=localhost
REDIS_PORT=6379
ALPACA_API_KEY=your-api-key
ALPACA_API_SECRET=your-api-secret
```

## 데이터 형식

모든 수집 데이터는 다음 형식으로 정규화됩니다:

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

## 개발 가이드

### 데이터 수집기 개발
- [collectors/README.md](collectors/README.md) 참조

### 백엔드 개발
- 각 모듈별 README 참조 (추후 추가 예정)

## 기술 스택

- **언어**: Python 3.9, Java 17
- **프레임워크**: Spring Boot 3.4.1, FastAPI
- **메시징**: Apache Kafka
- **데이터베이스**: TimescaleDB (PostgreSQL)
- **캐시**: Redis
- **컨테이너**: Docker, Docker Compose
- **배포**: Kubernetes (예정)

## 라이선스

캡스톤 디자인 프로젝트
