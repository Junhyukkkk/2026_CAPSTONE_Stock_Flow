# StockFlow 서버 운영 RUNBOOK

서버: `114.71.51.41` · SSH: `ssh -p 22000 capstone01@114.71.51.41`
스택 위치: `~/capstone/backend/infra` (docker compose 로 관리)
부하 테스트: `~/capstone/backend/perf`

전체 스택 = `docker compose` 관리 18개 서비스 + 예측 서비스(`stockflow-analysis`, 별도).

---

## 1. 브라우저 확인 주소

| 확인할 것 | 주소 |
|---|---|
| 앱 헬스 | http://114.71.51.41:8081/actuator/health |
| 앱 지표 원본 | http://114.71.51.41:8081/actuator/prometheus |
| 실시간 시세 화면 | http://114.71.51.41:8081/ui/live.html |
| 저장 적재 현황 | http://114.71.51.41:8081/storage-overview.html |
| Swagger | http://114.71.51.41:8081/swagger-ui/index.html |
| Grafana (admin/admin) | http://114.71.51.41:3000 |
| — 처리량·지연 | http://114.71.51.41:3000/d/stockflow-app |
| — 적체·유실 경보 | http://114.71.51.41:3000/d/stockflow-consumer-lag |
| — JVM | http://114.71.51.41:3000/d/stockflow-jvm |
| — Kafka | http://114.71.51.41:3000/d/stockflow-kafka |
| — Redis·HikariCP | http://114.71.51.41:3000/d/stockflow-data-infra |
| — 로그(Loki) | http://114.71.51.41:3000/d/stockflow-logs |
| Prometheus 경보 | http://114.71.51.41:9090/alerts |
| Prometheus 타깃 | http://114.71.51.41:9090/targets |
| Alertmanager | http://114.71.51.41:9093 |
| Kafka UI | http://114.71.51.41:8989 |
| RedisInsight | http://114.71.51.41:5540 |
| cAdvisor | http://114.71.51.41:8088 |
| 예측 API | http://114.71.51.41:8000 |

---

## 2. 개선 적용 확인 (SSH)

```bash
# 파티션 12개
docker exec stockflow-kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic market.normalized | head -1        # PartitionCount: 12

# Redis 2GB / 축출 0
docker exec stockflow-redis redis-cli info | grep -E 'maxmemory_human|evicted_keys'

# 개선 토글 (전부 true)
docker exec stockflow-realtime env | grep STOCKFLOW_OPT

# 로그 INFO (DEBUG 없음)
docker logs --tail 20 stockflow-realtime | grep -c DEBUG    # 0

# Consumer 적체
docker exec stockflow-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group realtime-group --describe
docker exec stockflow-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group storage-group --describe
```

---

## 3. 스택 조작

```bash
cd ~/capstone/backend/infra

docker compose ps                        # 상태
docker compose up -d                      # 전체 기동
docker compose --profile metrics up -d    # + exporter 4종 (Grafana Kafka/Redis/호스트 패널)
docker compose restart <service>          # 예: stockflow-realtime
docker compose logs -f stockflow-realtime
docker compose pull && docker compose up -d   # 이미지 갱신
docker compose down                       # 전체 정지 (명명 볼륨 = 데이터는 유지)
docker compose down -v                    # 정지 + 데이터 삭제 (주의)
```

앱 코드 갱신:
```bash
cd ~/capstone && git fetch && git reset --hard origin/main
cd backend/infra && docker compose up -d --build stockflow-realtime
```

---

## 4. 실시간/저장 프로세스 분리

CPU 여유가 더 필요할 때. 두 경로를 별도 컨테이너로 나눈다.

```bash
cd ~/capstone/backend/infra
echo 'STORAGE_CONSUMER_ENABLED=false' >> .env
echo 'BATCH_SCHEDULER_ENABLED=false'  >> .env
docker compose --profile split up -d
#  stockflow-realtime : 실시간 전용 (8081)
#  stockflow-storage  : 저장 + 배치 전용 (8082)
```

원복:
```bash
docker compose --profile split down
sed -i '/^STORAGE_CONSUMER_ENABLED=/d;/^BATCH_SCHEDULER_ENABLED=/d' .env
docker compose up -d
```

---

## 5. 부하 테스트

```bash
cd ~/capstone/backend/perf
./tps-sweep.sh <라벨>          # 초당 2천~1만 건 스윕 (~40분). 측정 중 수집기 자동 정지·복구
cat results/sweep_<라벨>_*/SUMMARY.md

# 특정 구간만
RATES="4000 5000 6000" HOLD=120 ./tps-sweep.sh quick

# 보관기간 초과 유실 재현 (retention 5분으로 임시 변경 후 원복)
./retention-cliff-test.sh
```

---

## 6. Discord 경보

- 경로: Prometheus 규칙 → Alertmanager → alertmanager-discord 브릿지 → Discord 채널
- 웹훅 URL 은 `backend/infra/.env` 의 `DISCORD_WEBHOOK` 에만 (repo·이미지에 없음)
- 규칙 3개: `ConsumerLagHigh`(적체>5만, 5분) / `ConsumerLagGrowing`(1만+ 지속 증가, 15분) / `ConsumerNearRetentionCliff`(유실 임박)
- 재알림 4시간마다, 해소 시 "resolved" 도 전송

동작 테스트 (가짜 경보 1건):
```bash
S=$(date -u +%Y-%m-%dT%H:%M:%SZ); E=$(date -u -d '+2 min' +%Y-%m-%dT%H:%M:%SZ)
curl -s -XPOST localhost:9093/api/v2/alerts -H 'Content-Type: application/json' \
 -d "[{\"labels\":{\"alertname\":\"Test\",\"severity\":\"warning\"},\"annotations\":{\"summary\":\"경보 테스트\"},\"startsAt\":\"$S\",\"endsAt\":\"$E\"}]"
```

---

## 7. 문제 대응

| 증상 | 확인 |
|---|---|
| 화면에 시세 안 뜸 | `docker compose ps` → stockflow-realtime, redis, kafka 상태 / `docker logs stockflow-realtime` |
| 적체 경보 왔다 | http://114.71.51.41:3000/d/stockflow-consumer-lag → 밀린 양·추세 / 수집량이 갑자기 늘었는지 |
| Grafana 패널 비어있음 | http://114.71.51.41:9090/targets 에서 exporter DOWN 이면 `docker compose --profile metrics up -d` |
| "화면 도달 시간" 음수 | 서버 시계 오차 (NTP 미동기, 관리자 권한 필요). 지표만 이상, 실제 처리는 정상 |
| 전체 재시작 | `cd ~/capstone/backend/infra && docker compose restart` |
