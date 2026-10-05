# StockFlow 서버 운영 RUNBOOK

서버: `114.71.51.41` · SSH: `ssh -p 22000 capstone01@114.71.51.41`
스택 위치: `~/capstone/backend/infra` (docker compose 로 관리)
부하 테스트: `~/capstone/backend/perf`

전체 스택 = `backend/infra/docker-compose.yml` 서비스(프로파일별 선택 기동) + 예측 서비스(`stockflow-analysis`, `analysis/docker-compose.yml` 별도).

## 빠른 참조 (하려는 일 → 절)

| 하려는 일 | 절 |
|---|---|
| 화면·Grafana·Prometheus 열기 | §1 |
| 배포 후 설정이 제대로 적용됐는지 확인 | §2 |
| 앱 갱신(CD 자동 배포 / 수동), 컨테이너 재생성 후 조치 | §3 |
| 저장 경로 옛 동작으로 롤백 | §3 (`.env` 에 옛 값) |
| CPU 가 모자랄 때 실시간/저장 분리 | §4 |
| 처리량 측정 | §5 |
| 경보 확인·테스트 | §6 |
| 증상별 확인 | §7 |
| 시뮬레이터 켜기/끄기, 테스트 데이터(SIMLOAD) 지우기 | §8 |

---

## 1. 브라우저 확인 주소

| 확인할 것 | 주소 |
|---|---|
| 앱 헬스 | http://114.71.51.41:8081/actuator/health |
| 앱 지표 원본 | http://114.71.51.41:8081/actuator/prometheus |
| 실시간 시세 화면 | http://114.71.51.41:8081/ui/live.html |
| 저장 적재 현황 | http://114.71.51.41:8081/storage-overview.html |
| Swagger | http://114.71.51.41:8081/swagger-ui/index.html |
| Grafana (admin/admin) | http://localhost:3000 |
| — 처리량·지연 | http://localhost:3000/d/stockflow-app |
| — 적체·유실 경보 | http://localhost:3000/d/stockflow-consumer-lag |
| — JVM | http://localhost:3000/d/stockflow-jvm |
| — Kafka | http://localhost:3000/d/stockflow-kafka |
| — Redis·HikariCP | http://localhost:3000/d/stockflow-data-infra |
| — 로그(Loki) | http://localhost:3000/d/stockflow-logs |
| Prometheus 경보 | http://localhost:9090/alerts |
| Prometheus 타깃 | http://localhost:9090/targets |
| Alertmanager | http://localhost:9093 |
| Kafka UI | http://localhost:8989 (`--profile kafka-ui` 로 켤 때만) |
| RedisInsight | http://localhost:5540 |
| cAdvisor | http://localhost:8088 |
| 예측 API | http://localhost:8000 |

### 관리 도구 접속 (SSH 터널)

외부에는 서비스 화면(8081)만 열려 있다. Grafana · Prometheus · Kafka UI · RedisInsight · 예측 API 등은
서버 내부(127.0.0.1)에서만 열리므로, 내 PC 에서 SSH 터널을 연 뒤 `localhost` 주소로 접속한다.

```bash
ssh -p 22000 -N -L 3000:localhost:3000 -L 9090:localhost:9090 -L 9093:localhost:9093 -L 5540:localhost:5540 -L 8000:localhost:8000 capstone01@114.71.51.41
```

터널을 연 터미널은 켜 둔다. Kafka UI 가 필요하면 서버에서 `docker compose --profile kafka-ui up -d kafka-ui`
로 켜고 `-L 8989:localhost:8989` 를 추가한다. 다 쓰면 `docker compose stop kafka-ui`.

> 2026-09-26: 인증 없이 외부에 공개돼 있던 Kafka UI 가 침입당해 채굴 프로그램이 돌았다.
> 관리 도구를 다시 외부에 열지 말 것.

### Redis 인증

| 항목 | 값 |
|---|---|
| 비밀번호 | 서버 `backend/infra/.env` 의 `REDIS_PASSWORD` (저장소에는 없음). 비우면 인증 없이 기동 — 로컬 개발 전용 |
| 접근 | 포트는 `127.0.0.1:6379` 만. 컨테이너 안 `docker exec stockflow-redis redis-cli …` 는 `REDISCLI_AUTH` 로 자동 인증 |
| 클라이언트 | 앱(realtime/storage)·redis-exporter·glitchtip 이 같은 변수 사용. RedisInsight 는 접속 정보에 비밀번호를 직접 입력 |
| 비밀번호 변경 | `.env` 수정 → `docker compose --profile metrics up -d redis stockflow-realtime redis-exporter`. `split` 프로파일을 쓰면 `stockflow-storage`, `sentry` 프로파일을 쓰면 glitchtip 도 함께 재생성 |
| Redis 재생성 후 | 캐시가 비므로 서버에서 `curl -X POST http://localhost:8081/api/batch/prev-close-sync` 로 전일 종가를 다시 적재(안 하면 등락률 0%). 응답의 `loadedSymbols` 가 0 이 아니어야 한다 |

- 2026-09-20 에는 인증 없는 Redis 가 외부 봇에 replica 로 바뀌어(`READONLY You can't write against a read only replica`) 쓰기가 전부 막혔다. 같은 증상이면 `redis-cli info replication` 에서 `role:slave` 인지 먼저 본다.
- 인증 확인: `docker exec stockflow-redis env -u REDISCLI_AUTH redis-cli ping` 이 `NOAUTH` 여야 한다.

---

## 2. 배포 후 설정 확인 (SSH)

컨테이너 env 의 기대값 (기본값 출처: `backend/infra/docker-compose.yml`):

| 항목 | 기대값 |
|---|---|
| `STOCKFLOW_OPT_*` | 전부 `true`, 단 `STOCKFLOW_OPT_REALTIME_BATCH=false` |
| `STOCKFLOW_IDEMPOTENCY_TTL_SECONDS` | 600 |
| `KAFKA_STORAGE_MAX_POLL_RECORDS` / `_FETCH_MIN_BYTES` / `_FETCH_MAX_WAIT_MS` | 500 / 65536 / 100 |
| `REDIS_PIPELINE_FLUSH` / `STOCKFLOW_E2E_SAMPLE_EVERY` | `each` / 1 |

`backend/infra/.env` 나 컨테이너 env 에 옛 고정값이 남아 있으면 기본값을 덮어쓴다. 아래 명령이 **아무것도 출력하지 않아야** 한다:

```bash
grep -nE '^(STOCKFLOW_OPT_STORAGE_IDEMPOTENCY_PIPELINE=false|STOCKFLOW_IDEMPOTENCY_TTL_SECONDS=86400|KAFKA_STORAGE_(MAX_POLL_RECORDS=100|FETCH_MIN_BYTES=1|FETCH_MAX_WAIT_MS=500))' ~/capstone/backend/infra/.env
```

env 로 보이지 않는 `application.yml` 값 확인: Hikari 24 → `curl -s localhost:8081/actuator/metrics/hikaricp.connections.max`, JDBC 옵션 → `docker logs stockflow-realtime 2>&1 | grep -m1 'reWriteBatchedInserts'`(Flyway 시작 로그). Lettuce 풀 48 은 `application.yml` 의 `spring.data.redis.lettuce.pool.max-active` 로만 확인된다.

```bash
# 파티션 12개
docker exec stockflow-kafka kafka-topics --bootstrap-server localhost:9092 \
  --describe --topic market.normalized | head -1        # PartitionCount: 12

# Redis 2GB (allkeys-lru) / 축출이 늘고 있지 않은지
docker exec stockflow-redis redis-cli info | grep -E 'maxmemory_human|evicted_keys'

# 개선 토글 + 저장 경로 튜닝 값 (위 표와 대조)
docker exec stockflow-realtime env | grep -E 'STOCKFLOW_|KAFKA_STORAGE|REDIS_PIPELINE'

# 분석 컨테이너 예약 (코어 6-7 / 메모리 3GB)
docker inspect stockflow-analysis --format '{{.HostConfig.CpusetCpus}} {{.HostConfig.Memory}}'   # 6,7 3221225472

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
- **자동**: `main` 에 병합(직접 push 포함)되면 `.github/workflows/deploy.yml` 이 빌드·테스트 후 서버에 SSH 로 접속해
  `git reset --hard origin/main` → `docker compose --profile metrics up -d --build` → `reserve-analysis-capacity.sh` 를 실행한다(Alpaca 수집기는 켜지 않는다. `analysis/` 는 별도 compose 라 배포 대상이 아니다: 분석 코드가 바뀌면 `cd ~/capstone/analysis && docker compose up -d --build`).
  서버(HDD·공용)의 이미지 빌드가 10분을 넘기므로 SSH 단계 timeout 은 40분이다. 배포는 한 번에 하나씩 순서대로 돈다.
- **수동**:
  ```bash
  cd ~/capstone && git fetch && git reset --hard origin/main
  cd backend/infra && docker compose up -d --build stockflow-realtime
  ```

컨테이너를 재생성(배포·`up -d --build`)한 뒤에는 분석 서비스 자원 예약을 다시 건다. 재생성하면 풀린다.
```bash
cd ~/capstone/backend/infra && ./reserve-analysis-capacity.sh   # 분석=코어 6,7·메모리 3GB, 나머지=코어 0-5
UNDO=1 ./reserve-analysis-capacity.sh                            # 해제
```

저장 경로 튜닝을 옛 동작으로 되돌릴 때 (`backend/infra/.env` 에 추가 후 `docker compose up -d stockflow-realtime`):
```bash
STOCKFLOW_IDEMPOTENCY_TTL_SECONDS=86400
KAFKA_STORAGE_MAX_POLL_RECORDS=100
KAFKA_STORAGE_FETCH_MIN_BYTES=1
KAFKA_STORAGE_FETCH_MAX_WAIT_MS=500
```
Lettuce 풀·Hikari·JDBC 옵션은 `application.yml` 기본값이지만 Spring 환경변수로 덮어쓸 수 있다: `SPRING_DATA_REDIS_LETTUCE_POOL_MAX_ACTIVE`, `SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE`, `SPRING_DATASOURCE_URL`(끝의 `?reWriteBatchedInserts=true` 를 빼고 지정).

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

도구·옵션 전체는 [backend/perf/SERVER_TEST.md](backend/perf/SERVER_TEST.md), 측정 결과는 [OPTIMIZATION_HISTORY.md](backend/perf/OPTIMIZATION_HISTORY.md).

```bash
cd ~/capstone/backend/perf
# 시뮬레이터 부하(source=SIMLOAD) + 분석 API 응답시간 프로브
~/capstone/backend/infra/reserve-analysis-capacity.sh          # 분석=코어 6,7 예약(먼저)
LOADGEN_MODE=sim SIM_CPUSET=0-5 ANALYSIS_PROBE=1 RATES="4000 8000 12000" ./tps-sweep.sh sim1
cat results/sweep_sim1_*/SUMMARY.md
```

- 측정 중 실수집기·라이브 시뮬레이터(`stockflow-stock-simulator`)는 자동 정지·복구된다(`STOP_COLLECTORS=1`).
- 끝나면 `SIMLOAD` 데이터를 지운다 → §8.
- 같은 서버·시간대에서 이어서 잰 상대 비교만 믿는다(공용 서버라 시간대별 절대값이 최대 2배 흔들린다).

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
| 적체 경보 왔다 | http://localhost:3000/d/stockflow-consumer-lag → 밀린 양·추세 / 수집량이 갑자기 늘었는지 |
| Grafana 패널 비어있음 | http://localhost:9090/targets 에서 exporter DOWN 이면 `docker compose --profile metrics up -d` |
| "화면 도달 시간" 음수 | 서버 시계 오차 (NTP 미동기, 관리자 권한 필요). 지표만 이상, 실제 처리는 정상 |
| `iowait` 급증·앱 전체가 느려짐 | 단일 HDD 서버. 대량 `DELETE`·연속 집계 갱신이 원인이면 중단하고 작은 시간 조각으로 나눠 다시(§8) |
| Redis `evicted_keys` 증가 | `maxmemory 2gb allkeys-lru`. 멱등성 TTL 이 600 인지 §2 로 확인(옛 값 86400 이면 가득 참) |
| 예측 API 가 비거나 오류 | HTTP 404 `예측에 필요한 시세 데이터가 부족합니다` = 해당 `(symbol, source)` 1분봉 50개 미만(시뮬레이터는 켠 뒤 약 1시간, `SIM_MARKET_HOURS=us` 면 장중에만). 503 `예측 서비스에 연결할 수 없습니다` = analysis 컨테이너 확인. 502 = 분석 서비스가 빈/잘못된 응답. 첫 호출은 학습으로 10~15초(Java 읽기 timeout 120초). 시뮬레이터 종목 추가는 `collectors/simulator/universe.csv` 에 행을 추가하고 컨테이너 재시작 |
| 예측 이력이 안 쌓임 | `PREDICTION_HISTORY_ENABLED` 확인, `prediction_history_dropped_total` 지표(큐 포화) 확인. 조회: `GET /api/predictions/{symbol}/history?interval=1m&limit=20`(파라미터는 interval·limit 만). 직접 보기: `SELECT symbol, interval, source, base_ts, requested_at, latency_ms FROM prediction_runs ORDER BY requested_at DESC LIMIT 10;` |
| CD 배포가 중간에 실패·timeout | ① Actions 로그의 SSH 단계 확인(서버는 이미지 빌드가 10분 넘음, timeout 40분). ② 서버에서 `docker ps -a` 로 멈춘 컨테이너(`Exited`)와 이름이 `해시_이름` 인 `Created` 임시 컨테이너를 찾아, 멈춘 것은 `docker start`, 임시 것은 `docker rm`. ③ 재실행은 Actions → Deploy to server → Run workflow(`workflow_dispatch`) 또는 §3 수동 절차. ④ 끝나면 `reserve-analysis-capacity.sh`, Redis 가 재생성됐다면 `prev-close-sync`, 시뮬레이터가 떠 있고 `stockflow-alpaca-collector` 는 꺼져 있는지 확인 |
| 배포 뒤 분석 응답이 느려짐 | 컨테이너가 재생성돼 CPU 예약이 풀렸다 → `./reserve-analysis-capacity.sh` (§3) |
| 전체 재시작 | `cd ~/capstone/backend/infra && docker compose restart` |

---

## 8. 시뮬레이터 · 테스트 데이터

### 라이브 시뮬레이터 켜기/끄기

서버의 라이브 시뮬레이터는 compose 가 아니라 `docker run` 으로 띄운다(이미지 빌드 없이 레포 코드를 마운트). 현재 `SIM_RATE_SCALE=1.0`(105종목 평균 약 832/s).

```bash
# 환경변수는 파일로 전달(키 값이 명령행·로그에 남지 않게): KAFKA_BOOTSTRAP_SERVERS=kafka:9092, KAFKA_TOPIC_NAME=market.normalized,
# SIM_RATE_MODE=realistic, SIM_RATE_SCALE=1.0, SIM_MARKET_HOURS=always, SIM_PRICE_SOURCE=auto,
# ALPACA_API_KEY, ALPACA_API_SECRET, PYTHONPATH=/app, HEALTH_FILE_PATH=/tmp/sim_health.json
docker run -d --name stockflow-stock-simulator --restart unless-stopped --network infra_default \
  --memory 256m --cpus 1 --cpuset-cpus 0-5 -v ~/capstone/collectors:/app:ro -w /app \
  --env-file ~/sim.env collectors-binance-collector:latest python stock_simulator.py
docker logs --tail 3 stockflow-stock-simulator        # 속도 약 830 msg/s 확인
docker stop stockflow-stock-simulator                 # 끄기
```

- 개발용 compose 서비스(`docker compose --profile sim up -d stock-simulator`)의 기본 `SIM_RATE_SCALE` 은 0.25(약 208/s). 상한 100. 라벨 기본 `SIMULATOR`(`BINANCE`/`ALPACA` 는 거부). 변수 전체는 [collectors/README.md](collectors/README.md#주요-환경-변수).
- `alpaca-collector` 는 시뮬레이터와 같은 티커를 쓰므로 함께 돌리지 않는다(Redis 최신가·`instruments.exchange` 가 섞인다). 배포 워크플로는 `--profile alpaca` 를 켜지 않는다.
- 예측은 `(symbol, source)` 1분봉 50개 이상이 필요해 켠 뒤 약 1시간 후부터 가능하다.

### 부하 테스트 데이터(`SIMLOAD`) 지우기

`tps-sweep.sh LOADGEN_MODE=sim` 이 만든 행(`source='SIMLOAD'`)을 아래 순서로 지운다. 순서가 중요하다.
단일 HDD 라 한 번에 크게 지우면 iowait 로 서비스 전체가 느려지므로 **짧은 시간 조각**으로 나눈다.
`market_ticks` 는 7일 뒤 압축되므로 그 전에 지운다. 시작·종료 시각은 스윕 결과 `timeline.csv` 의 `epoch`(서버 시계) 첫·끝 값을 쓴다.

```bash
PSQL="docker exec -i stockflow-timescaledb psql -U postgres -d stockflow"
FROM=$(date -u -d '2026-10-05 02:00' +%s); TO=$(date -u -d '2026-10-05 04:00' +%s)

# 1) 생성기 정지 (스윕이 끝났는지, stockflow-loadgen-sim-* 컨테이너가 없는지 확인)
docker ps --format '{{.Names}}' | grep loadgen-sim

# 2) market_ticks: 10분 조각으로 삭제
for ((t=FROM; t<TO; t+=600)); do
  $PSQL -c "DELETE FROM market_ticks WHERE source='SIMLOAD' AND ts >= to_timestamp($t) AND ts < to_timestamp($t+600)"
  sleep 5
done

# 3) 연속 집계 market_ticks_1m 을 같은 구간만 갱신
$PSQL -c "CALL refresh_continuous_aggregate('market_ticks_1m', to_timestamp($FROM), to_timestamp($TO))"

# 4) 파생 테이블 (market-data-sync 가 만든 1분봉, 일봉 배치가 만든 일봉)
# 시간 범위를 반드시 준다: 범위 없는 DELETE 는 모든 청크를 읽어 HDD 에서 10분 넘게 걸린다
$PSQL -c "DELETE FROM ohlcv_1m WHERE source='SIMLOAD' AND bucket >= to_timestamp($FROM) AND bucket < to_timestamp($TO) + interval '1 hour'"
$PSQL -c "DELETE FROM symbol_daily_ohlcv WHERE source='SIMLOAD'"
```

확인: `$PSQL -c "SELECT count(*) FROM ohlcv_1m WHERE source='SIMLOAD' AND bucket >= to_timestamp($FROM)"` 가 0 이어야 한다. `market_ticks` 는 시간 범위 없이 `count(*)` 하지 않는다(HDD 에서 10분 이상).
