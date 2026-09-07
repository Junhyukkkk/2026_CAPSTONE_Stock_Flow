# Grafana 대시보드 "중간중간 끊김" 원인 정리

서버(114.71.51.41)에서 확인한 것. 3가지가 겹쳐 있다.

## 1. 없는 exporter → 패널 자체가 no-data (가장 큰 원인)

`infra/prometheus/prometheus.yml` 은 **앱 1개(`stockflow-realtime:8081`)만** 스크레이프한다.
kafka-exporter / redis-exporter / node-exporter / cAdvisor 가 배포에 없다.

그래서 대시보드 중 아래 패널들은 데이터 소스 자체가 없어 "No data" 로 뜨거나
간헐적으로만 채워진 것처럼 보인다:

| 대시보드 | 비어 있는 패널 | 필요한 exporter |
| --- | --- | --- |
| 01-kafka | broker/topic 처리량, under-replicated 등 | kafka-exporter |
| 03-redis-hikari | Redis 메모리·hit rate·연결 (Hikari 부분만 앱 지표라 나옴) | redis-exporter |
| 02-jvm | 대부분 앱 지표라 정상. 호스트 CPU/메모리 패널만 빔 | node-exporter |

**조치**: `docker-compose.yml` 에 `metrics` 프로파일로 exporter 4종 추가해 뒀다.
```bash
cd backend/infra
docker compose --profile metrics up -d
```
`prometheus.yml` 에 스크레이프 잡도 추가돼 있다(프로파일 안 켜면 target=down, 스택엔 무해).

## 2. `stockflow_e2e_latency` 지표가 깨져 있음

서버 `/actuator/prometheus`:
```
stockflow_e2e_latency_p50  -859413
stockflow_e2e_latency_p90  -859302
stockflow_e2e_latency_p99  -858677
stockflow_e2e_latency_distribution_ms{quantile="0.5"}  0.0   ← 히스토그램은 음수를 0으로 clamp
```
**음수(-14분).** E2E = `now - trade.timestamp` 인데 `trade.timestamp`(거래소 이벤트 시각)가
서버 시계보다 미래다 = **서버 시계가 뒤처져 있음**(NTP 미동기 의심) + 코드가 음수를 안 막음.

→ 그래서 "거래소→화면 지연" 패널이 비거나 0으로 나온다.

**조치(코드/운영)**:
- 서버 NTP 동기화 (`timedatectl`, `chrony`)
- `PerformanceMetrics` 에서 e2e 음수면 버리거나 `receivedAt` 기준으로 계산

## 3. 서버가 구버전 코드 → 커스텀 지표 자체가 없음

서버 HEAD = `1f814a5` (성능 계측 커밋 `3fdb434`, 모니터링 커밋들 이전).
- `stockflow_stage_seconds{stage=...}` (구간별 지연) — **없음**
- `stockflow_consumer_lag` / `stockflow_consumer_retention_margin` (유실 경보) — **없음**
- consumer_lag 경보 규칙(`prometheus/rules/consumer_lag.yml`)도 서버엔 없음

→ "Consumer Lag & 유실 위험" 대시보드가 통째로 빈다.

**조치**: 최신 `main` 배포(`git pull` + `docker compose up -d --build`).

## 4. (부차) 단일 타깃 + 5초 스크레이프

`stockflow-realtime` 하나만, `scrape_interval: 5s`. 앱이 GC 로 길게 멈추거나
(부하 테스트 중 `stockflow_processing_time_max` 9.5초 관측) 재시작하면
그 구간이 그래프에서 끊긴다. exporter 추가 + 앱 튜닝으로 완화된다.

---

## 요약: 지금 해야 할 것

1. `docker compose --profile metrics up -d` — kafka/redis/host 패널 채움
2. 최신 `main` 배포 — stage/consumer_lag 지표 + 경보 살아남
3. 서버 NTP 동기화 + e2e 지표 음수 가드 (코드)
