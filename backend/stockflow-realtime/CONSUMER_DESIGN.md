# Kafka Consumer 설계 문서

## 1. 전체 아키텍처

### Consumer Group 분리 전략

```
Producer → Kafka Topic → Consumer Group 1 (실시간용) → Redis
                      → Consumer Group 2 (저장용) → PostgreSQL (TimescaleDB)
```

### Consumer Group 역할

| Consumer Group | 목적 | 저장소 | 처리 방식 |
|---------------|------|--------|----------|
| `realtime-group` | 실시간 데이터 제공 | Redis | 단일 메시지 처리 |
| `storage-group` | 영구 저장 | PostgreSQL | 배치 처리 (성능 최적화) |

---

## 2. Consumer Group 1: 실시간용 (Realtime Consumer)

### 역할
- 실시간 데이터를 Redis에 저장
- WebSocket을 통해 클라이언트에 전송
- 낮은 지연시간이 중요

### 특징
- **처리 방식**: 단일 메시지 처리 (실시간성)
- **저장소**: Redis (Sorted Set, Hash)
- **Consumer Group**: `realtime-group`
- **토픽**: `market.normalized`
- **Offset 관리**: 수동 커밋 (enable-auto-commit: false)

### 데이터 구조 (Redis)
```
# 실시간 가격 (Sorted Set)
key: "price:{symbol}"
value: {price, timestamp, volume}

# 최신 틱 데이터 (Hash)
key: "tick:{symbol}:latest"
value: {price, volume, timestamp, source}
```

---

## 3. Consumer Group 2: 저장용 (Storage Consumer)

### 역할
- 데이터를 PostgreSQL (TimescaleDB)에 영구 저장
- 배치 처리로 성능 최적화
- 분석 및 백테스팅용 데이터 제공

### 특징
- **처리 방식**: 배치 처리 (성능 최적화)
- **저장소**: PostgreSQL (TimescaleDB)
- **Consumer Group**: `storage-group`
- **토픽**: `market.normalized`
- **배치 크기**: 100~1000건 (설정 가능)

### 데이터 구조 (PostgreSQL)
```sql
CREATE TABLE market_ticks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    source VARCHAR(20) NOT NULL,
    price DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- TimescaleDB 하이퍼테이블 변환
SELECT create_hypertable('market_ticks', 'timestamp');
```

---

## 4. 에러 처리 전략

### DLQ (Dead Letter Queue) 전략

#### 전송 조건
1. **재시도 실패**: 최대 재시도 횟수 초과
2. **데이터 검증 실패**: 잘못된 형식의 메시지
3. **저장소 연결 실패**: Redis/DB 연결 불가
4. **처리 시간 초과**: 타임아웃 발생

#### DLQ 메시지 구조
```json
{
  "originalTopic": "market.normalized",
  "originalPartition": 0,
  "originalOffset": 12345,
  "originalMessage": {...},
  "errorType": "VALIDATION_ERROR",
  "errorMessage": "Invalid price format",
  "failedAt": "2024-01-01T12:00:00Z",
  "retryCount": 3
}
```

### 재시도 전략

#### Exponential Backoff
- **초기 지연**: 1초
- **최대 지연**: 60초
- **최대 재시도**: 3회
- **배수**: 2배씩 증가

#### 재시도 시나리오
1. **일시적 오류** (네트워크, DB 연결): 재시도
2. **영구적 오류** (데이터 형식 오류): DLQ 전송
3. **시스템 오류** (메모리 부족): DLQ 전송 + 알림

---

## 5. 성능 최적화

### 배치 처리 (Storage Consumer)

#### 배치 수집 전략
- **시간 기반**: 1초마다 배치 처리
- **크기 기반**: 100건 모이면 즉시 처리
- **하이브리드**: 시간 또는 크기 중 먼저 도달하는 조건

#### 배치 처리 흐름
```
메시지 수신 → 메모리 버퍼에 추가
           → 배치 조건 확인 (시간/크기)
           → 조건 만족 시 DB 일괄 저장
           → Offset 커밋
```

### 병렬 처리

#### 파티션별 병렬 처리
- 각 파티션은 독립적으로 처리
- 파티션 수 = Consumer 인스턴스 수 (권장)

#### 동시성 설정
```yaml
spring:
  kafka:
    listener:
      concurrency: 4  # 파티션 수와 동일하게
```

---

## 6. 트랜잭션 처리

### 수동 Offset 커밋 전략

#### 커밋 시점
1. **성공 시**: 메시지 처리 완료 후 즉시 커밋
2. **실패 시**: 커밋하지 않음 (재처리 가능)
3. **배치 처리**: 배치 전체 성공 후 일괄 커밋

#### 트랜잭션 보장
- **At-Least-Once**: 메시지 중복 가능 (idempotent 처리 필요)
- **Exactly-Once**: 배치 처리 시 트랜잭션 사용

---

## 7. 모니터링 및 메트릭

### 필수 메트릭

#### Consumer 메트릭
- **Lag**: 처리 못한 메시지 수
- **Throughput**: 초당 처리 메시지 수
- **Error Rate**: 실패율
- **Processing Time**: 메시지당 처리 시간

#### 저장소 메트릭
- **Redis**: 연결 상태, 메모리 사용량
- **PostgreSQL**: 연결 상태, 쿼리 성능

### 로깅 전략
- **INFO**: 정상 처리 로그 (요약)
- **WARN**: 재시도, 일시적 오류
- **ERROR**: DLQ 전송, 시스템 오류
- **DEBUG**: 상세 처리 로그 (개발 환경)

---

## 8. 헬스체크

### Health Check Endpoint
- **Path**: `/actuator/health`
- **체크 항목**:
  - Kafka 연결 상태
  - Redis 연결 상태
  - PostgreSQL 연결 상태
  - Consumer Group 상태

---

## 9. 구현 단계

### 1단계: 기본 Consumer 구조
- [ ] RealtimeConsumer 구현
- [ ] StorageConsumer 구현
- [ ] Consumer Group 분리
- [ ] 기본 메시지 처리

### 2단계: 에러 처리
- [ ] DLQ 전송 로직
- [ ] 에러 분류 (일시적/영구적)
- [ ] 에러 로깅

### 3단계: 재시도 전략
- [ ] Exponential Backoff 구현
- [ ] 재시도 로직
- [ ] 최대 재시도 제한

### 4단계: 성능 최적화
- [ ] 배치 처리 구현
- [ ] 병렬 처리 설정
- [ ] 성능 튜닝

### 5단계: 트랜잭션
- [ ] 수동 Offset 커밋
- [ ] 배치 트랜잭션
- [ ] Idempotent 처리

### 6단계: 모니터링
- [ ] 메트릭 수집
- [ ] 헬스체크 구현
- [ ] 로깅 개선

### 7단계: 통합 테스트
- [ ] End-to-End 테스트
- [ ] 부하 테스트
- [ ] 장애 복구 테스트

---

## 10. 설정 예시

### application.yml
```yaml
spring:
  kafka:
    consumer:
      group-id: ${CONSUMER_GROUP_ID:realtime-group}
      auto-offset-reset: latest
      enable-auto-commit: false
      max-poll-records: 100
      fetch-min-size: 1
      fetch-max-wait: 500ms
    listener:
      concurrency: 4
      ack-mode: manual
      type: batch  # Storage Consumer용
```

---

## 11. 보안 고려사항

### 인증/인가
- Kafka SASL/SCRAM 인증 (프로덕션)
- SSL/TLS 암호화

### 데이터 검증
- 메시지 스키마 검증
- 가격/볼륨 범위 검증
- 타임스탬프 검증

---

## 12. 확장성 고려사항

### 수평 확장
- Consumer 인스턴스 추가 가능
- 파티션 수 조정으로 처리량 증가

### 수직 확장
- 배치 크기 조정
- 메모리 버퍼 크기 조정
