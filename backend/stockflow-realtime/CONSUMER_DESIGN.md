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
| `realtime-group` | 실시간 데이터 제공 | Redis | 단일 메시지 처리 (`STOCKFLOW_OPT_REALTIME_BATCH=true` 면 poll 단위 배치) |
| `storage-group` | 영구 저장 | PostgreSQL | 배치 처리 ([설정 빠른 참조](#설정-빠른-참조-저장실시간-경로)) |

## 설정 빠른 참조 (저장·실시간 경로)

기본값은 `application.yml`·`docker-compose.yml`·코드에서 확인한 값이다. env 로 덮어쓴다(compose 는 `${VAR:-기본값}` 형태).

| 바꾸고 싶은 것 | env | 기본값 | 옛 값(되돌릴 때) | 효과·근거 |
|---|---|---|---|---|
| 멱등성 키 TTL(초) | `STOCKFLOW_IDEMPOTENCY_TTL_SECONDS` | 600 | 86400 | Redis 2GB 가 키로 가득 차 축출되는 것을 막는다. 0 이하면 기동 실패 |
| 저장 컨슈머 poll 크기 | `KAFKA_STORAGE_MAX_POLL_RECORDS` | 500 | 100 | 저장 경로 묶음 효과 아래 참조 |
| 저장 컨슈머 fetch 최소 바이트 | `KAFKA_STORAGE_FETCH_MIN_BYTES` | 65536 | 1 | 〃 |
| 저장 컨슈머 fetch 최대 대기(ms) | `KAFKA_STORAGE_FETCH_MAX_WAIT_MS` | 100 | 500 | 〃 |
| 저장 멱등성 체크 파이프라인 | `STOCKFLOW_OPT_STORAGE_IDEMPOTENCY_PIPELINE` | true | false | 배치 전체를 Redis 1왕복 |
| 실시간 poll 배치 소비 | `STOCKFLOW_OPT_REALTIME_BATCH` | false | - | 측정상 이득 없어 꺼 둠 |
| Redis 파이프라인 flush | `REDIS_PIPELINE_FLUSH` | `each` (`close`, `buffered:N`) | - | 측정상 이득 없어 기본 유지 |
| E2E 지연 샘플링 간격 | `STOCKFLOW_E2E_SAMPLE_EVERY` | 1 (매 건) | - | N 이면 N건마다 1건 계측 |
| Lettuce 풀 `max-active` / Hikari 풀 / JDBC | `application.yml` 고정값 | 48 / 24 / `reWriteBatchedInserts=true` | 16 / Hikari 기본(10) / 옵션 없음 | `.env` 로는 안 바뀐다(compose 가 `environment:` 목록의 변수만 전달). compose `environment:` 에 `SPRING_DATA_REDIS_LETTUCE_POOL_MAX_ACTIVE`·`SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE`·`SPRING_DATASOURCE_URL`(전체 JDBC URL) 줄을 추가하거나 `docker run -e` 로 지정 ([RUNBOOK.md §3](../../RUNBOOK.md#3-스택-조작)) |

저장 경로 묶음(TTL 600s + 저장 배치 500/64KB/100ms + `reWriteBatchedInserts` + Lettuce 풀 48 + Hikari 24 + 멱등성 파이프라인)은
합쳐서 켤 때만 효과가 있다: 합성 부하 10,000/s 에서 저장 소비 6,000 → 9,000/s, 시뮬레이터 부하에서 저장 상한 약 6,600 → 약 14,000/s
([OPTIMIZATION_HISTORY.md §2](../perf/OPTIMIZATION_HISTORY.md)). 서버 `backend/infra/.env` 나 컨테이너 env 에 옛 고정값
(예: `STOCKFLOW_OPT_STORAGE_IDEMPOTENCY_PIPELINE=false`)이 남아 있으면 새 기본값을 덮어쓴다.

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
# 최신가 (String, TTL 60초) — PriceKeys.LATEST_PRICE_PREFIX
key: "price:latest:{symbol}"

# 실시간 전송 (Pub/Sub 채널) — PriceKeys.PRICE_CHANNEL_PREFIX
channel: "price:{symbol}"

# 멱등성 키 (String, TTL 기본 600초)
key: "processed:{channel}:{symbol}:{source}:{tradeId}:{timestamp}"
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
- **배치 크기**: poll 당 최대 `KAFKA_STORAGE_MAX_POLL_RECORDS`(기본 500)건 — 위 설정 표
- **중복 방지**: DB 유니크 키 `(symbol, source, trade_id, ts)` + `ON CONFLICT DO NOTHING`. Redis 멱등성 키는 재전달 시 DB 왕복을 줄이는 용도라 TTL 이 짧아도(축출돼도) 정합성은 유지된다.

### 데이터 구조 (PostgreSQL)
`market_ticks` 하이퍼테이블(`ts` 기준) — 정의는 `src/main/resources/db/migration/V1__create_market_ticks.sql`.

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
Spring Kafka 배치 리스너가 poll 한 묶음을 그대로 한 배치로 처리한다(별도 메모리 버퍼 없음).
poll 크기·대기는 위 설정 표의 `KAFKA_STORAGE_*` 로 조정한다(최대 500건, 64KB 모이거나 100ms 지나면 반환).

#### 배치 처리 흐름
```
poll(최대 500건) → 멱등성 체크(Redis 파이프라인 1왕복)
                → DB 일괄 INSERT (reWriteBatchedInserts, ON CONFLICT DO NOTHING)
                → 멱등성 키 마킹(파이프라인) → Offset 커밋
```

### 병렬 처리

#### 파티션별 병렬 처리
- 각 파티션은 독립적으로 처리
- 파티션 수 = Consumer 인스턴스 수 (권장)

#### 동시성 설정
`KAFKA_CONSUMER_CONCURRENCY`(기본 12) = `market.normalized` 파티션 수.

---

## 6. 트랜잭션 처리

### 수동 Offset 커밋 전략

#### 커밋 시점
1. **성공 시**: 메시지 처리 완료 후 즉시 커밋
2. **실패 시**: 커밋하지 않음 (재처리 가능)
3. **배치 처리**: 배치 전체 성공 후 일괄 커밋

#### 트랜잭션 보장
- **At-Least-Once**: 메시지 중복 가능 → Redis 멱등성 키 + DB `ON CONFLICT DO NOTHING` 으로 흡수

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

## 9. 구현 위치

| 역할 | 클래스 (`com.stockflow.realtime`) |
|---|---|
| 실시간 소비 | `consumer.RealtimeConsumer` (단건), `consumer.RealtimeBatchConsumer` (`STOCKFLOW_OPT_REALTIME_BATCH=true`) |
| 저장 소비 | `consumer.StorageConsumer` → `storage` 패키지 (`MarketTickBulkWriter`) |
| 멱등성 | `transaction.IdempotencyService` |
| 컨테이너·팩토리 설정 | `config.KafkaConsumerConfig` (기본 팩토리 / `storageConsumerFactory`) |

---

## 10. 설정 예시

### application.yml
실제 값은 `backend/stockflow-realtime/src/main/resources/application.yml` 이 원본이다. 실시간 컨슈머는 `max-poll-records` 100 /
`fetch-min-size` 1 / `fetch-max-wait` 500ms, 저장 컨슈머는 위 설정 표의 `KAFKA_STORAGE_*` 값을 쓴다. 둘 다 수동 커밋(`ack-mode: manual`).

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
