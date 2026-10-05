# 실시간 테스트 확인 가이드

## 1. Kafka UI로 확인

### 접속 방법
Kafka UI 는 외부에 열려 있지 않고 기본 기동도 안 된다. 서버에서 켠 뒤 SSH 터널로 접속한다 ([RUNBOOK.md §1](RUNBOOK.md)):
```bash
docker compose --profile kafka-ui up -d kafka-ui     # 서버 ~/capstone/backend/infra
ssh -p 22000 -N -L 8989:localhost:8989 capstone01@114.71.51.41   # 내 PC
# 브라우저: http://localhost:8989 (다 쓰면 docker compose stop kafka-ui)
```

### 확인할 내용
1. **Topics** 메뉴 클릭
2. `market.normalized` 토픽 선택
   - Messages 탭에서 실시간 메시지 확인
   - 파티션별 메시지 수 확인
3. **Consumers** 메뉴에서 `realtime-group`, `storage-group` lag 확인

---

## 2. 실시간 로그 확인

### Producer 로그 (Binance)
```bash
docker logs -f stockflow-binance-collector
```
- 실시간으로 메시지 전송 로그 확인
- 통계 정보 확인 (전송 건수, 속도 등)

### Producer 로그 (주식 시뮬레이터, `--profile sim` 으로 켠 경우)
```bash
docker logs -f stockflow-stock-simulator 2>&1 | grep -E "합계 평균 TPS|📊"
```
- 시작 로그의 `합계 평균 TPS` = 기대 속도, `📊` 줄 = 주기 통계 (종료 시 `최종 통계`)

### Consumer 로그 (Spring Boot)
```bash
docker logs -f stockflow-realtime
```
- 메시지 수신 로그 확인
- 처리 로그 확인

---

## 3. Kafka에서 직접 메시지 확인

### market.normalized 토픽 메시지 확인
```bash
cd /home/capstone01/capstone/backend/infra
docker exec -it stockflow-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market.normalized \
  --from-beginning \
  --max-messages 5
```

---

## 4. 메시지 개수 확인

### 현재 메시지 개수 확인
```bash
cd /home/capstone01/capstone/backend/infra
docker exec stockflow-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic market.normalized
```

---

## 5. Consumer 상태 확인

### Consumer Group 상태 확인
```bash
cd /home/capstone01/capstone/backend/infra
docker exec stockflow-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

### Consumer Lag 확인
```bash
cd /home/capstone01/capstone/backend/infra
docker exec stockflow-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group realtime-group \
  --describe
```

---

## 6. Spring Boot Actuator로 확인

### Health Check
```bash
curl http://114.71.51.41:8081/actuator/health
```

### Metrics 확인
```bash
curl http://114.71.51.41:8081/actuator/metrics
```

### Consumer 메트릭 확인
```bash
curl http://114.71.51.41:8081/api/metrics
```

### 예측 호출과 이력 저장 확인
```bash
curl "http://114.71.51.41:8081/api/predictions/AAPL/compare?interval=1m&horizon=10&source=SIMULATOR"
curl "http://114.71.51.41:8081/api/predictions/AAPL/history?interval=1m&limit=5"   # 방금 호출한 결과가 저장돼 있어야 함
```
- 해당 `(symbol, source)` 1분봉이 50개 미만이면 예측이 안 나온다. 모델 캐시가 없는 종목의 첫 호출은 10~15초.
- 저장은 비동기라 직후 조회에서 한두 번 비어 있을 수 있다. 계속 비면 큐 포화 카운터 `prediction_history_dropped_total` 확인.

---

## 빠른 확인 명령어

### 모든 컨테이너 상태 확인
```bash
docker ps | grep stockflow
```

### Producer 실시간 로그
```bash
docker logs -f stockflow-binance-collector | grep -E "전송|통계|ERROR"
```

### Consumer 실시간 로그
```bash
docker logs -f stockflow-realtime | grep -E "Received|Processing|trade"
```
