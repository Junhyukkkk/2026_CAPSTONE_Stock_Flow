# Kafka Topic 설계 문서

## 1. 네이밍 규칙

### 패턴
```
market.<source>.<data-type>
```

### 구성 요소
- `market`: 도메인 접두사 (주식/암호화폐 시장 데이터)
- `<source>`: 데이터 소스 (binance, alpaca, coinbase 등)
- `<data-type>`: 데이터 타입 (tick, quote, trade 등)

### 예시
- `market.binance.tick`: Binance 실시간 체결 데이터
- `market.alpaca.tick`: Alpaca 실시간 체결 데이터
- `market.normalized`: 정규화된 통합 데이터 (Consumer가 변환)
- `market.dlq`: Dead Letter Queue (실패한 메시지)

## 2. 파티션 전략

### 파티션 수 결정 기준
1. **처리량**: 초당 메시지 수 (msg/s)
2. **병렬 처리**: Consumer 수
3. **확장성**: 향후 증가 예상량

### 권장 파티션 수

| Topic | 파티션 수 | 이유 |
|-------|----------|------|
| `market.binance.tick` | 6 | 300종목, 초당 수천 건 예상 |
| `market.alpaca.tick` | 12 | 전체 종목, 초당 만 건 이상 예상 |
| `market.normalized` | 12 | 통합 데이터, 높은 처리량 필요 |
| `market.dlq` | 3 | 실패 메시지는 상대적으로 적음 |

### 파티션 키 전략
- **Key**: `symbol` (종목 심볼)
- **이유**: 
  - 같은 종목의 메시지는 같은 파티션으로 라우팅
  - 순서 보장 (같은 종목 내에서)
  - Consumer에서 종목별 병렬 처리 가능

### 파티션 수 조정 가이드
```bash
# 파티션 수 = Consumer 수 × 처리량 여유분 (1.5~2배)
# 예: Consumer 4개 → 파티션 6~8개 권장
```

## 3. Retention 정책

### Retention 시간
- **실시간 데이터 토픽** (`market.*.tick`): 4시간
  - 이유: 실시간 처리 중심, 오래된 데이터 불필요
- **정규화 토픽** (`market.normalized`): 4시간
  - 이유: Consumer가 빠르게 소비 후 DB 저장
- **DLQ** (`market.dlq`): 7일
  - 이유: 디버깅 및 재처리 필요

### 설정
```bash
# retention.ms (밀리초)
market.binance.tick: 14400000 (4시간)
market.alpaca.tick: 14400000 (4시간)
market.normalized: 14400000 (4시간)
market.dlq: 604800000 (7일)
```

## 4. Replication Factor

### 개발 환경
- **Replication Factor: 1**
- 이유: 단일 브로커 환경

### 프로덕션 환경 (권장)
- **Replication Factor: 3**
- 이유: 고가용성 및 데이터 안정성

## 5. Compression

### Producer 설정
- **Compression Type: snappy**
- 이유: 
  - 빠른 압축/해제 속도
  - 네트워크 대역폭 절약
  - CPU 오버헤드 적음

## 6. Topic 생성 스크립트

`backend/infra/create-topics.sh` 참조

## 7. 모니터링 지표

### 필수 모니터링 항목
1. **Lag**: Consumer가 처리 못한 메시지 수
2. **Throughput**: 초당 메시지 수 (in/out)
3. **Error Rate**: 실패율
4. **Partition Size**: 파티션별 메시지 수

### Kafka UI
- URL: http://localhost:8989
- Topic별 상세 메트릭 확인 가능

## 8. 확장 전략

### 향후 추가 가능한 토픽
- `market.binance.quote`: Binance 호가 데이터
- `market.alpaca.quote`: Alpaca 호가 데이터
- `market.aggregated.1min`: 1분봉 집계 데이터
- `market.aggregated.5min`: 5분봉 집계 데이터

### 파티션 확장
```bash
# 파티션 수 증가 (주의: 키 기반 순서 보장에 영향)
kafka-topics --alter --bootstrap-server localhost:9092 \
  --topic market.binance.tick --partitions 12
```