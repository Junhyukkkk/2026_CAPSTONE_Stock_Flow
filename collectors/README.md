# StockFlow Data Collectors

실시간 주식/암호화폐 데이터 수집기 (Binance + Alpaca)

## 구조

```
collectors/
├── binance_producer.py    # Binance 암호화폐 데이터 수집
├── alpaca_producer.py     # Alpaca 주식 데이터 수집
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
- 상위 거래량 300개 코인 실시간 수집
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

## 설정

### 환경 변수

`.env` 파일을 생성하거나 환경 변수로 설정:

```bash
# Kafka 설정
KAFKA_BOOTSTRAP_SERVERS=kafka:9093

# Binance 설정
BINANCE_TOP_SYMBOLS_LIMIT=300
BINANCE_TOPIC_NAME=market.binance.tick

# Alpaca 설정 (필수)
ALPACA_API_KEY=your-api-key
ALPACA_API_SECRET=your-api-secret
ALPACA_TOPIC_NAME=market.alpaca.tick
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
cd ../../collector-python
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

- `market.binance.tick`: Binance 암호화폐 데이터 (6 파티션, 4시간 retention)
- `market.alpaca.tick`: Alpaca 주식 데이터 (12 파티션, 4시간 retention)
- `market.dlq`: Dead Letter Queue - 실패한 메시지 (3 파티션, 7일 retention)

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
