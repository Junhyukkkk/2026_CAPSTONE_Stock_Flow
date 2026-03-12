# 실시간 테스트 확인 가이드

## 1. Kafka UI로 확인

### 접속 방법
브라우저에서 접속:
```
http://114.71.51.41:8989
```

### 확인할 내용
1. **Topics** 메뉴 클릭
2. `market.binance.tick` 토픽 선택
   - Messages 탭에서 실시간 메시지 확인
   - 파티션별 메시지 수 확인
3. `market.normalized` 토픽 확인
   - Consumer가 메시지를 받는지 확인

---

## 2. 실시간 로그 확인

### Producer 로그 (Binance)
```bash
docker logs -f stockflow-binance-collector
```
- 실시간으로 메시지 전송 로그 확인
- 통계 정보 확인 (전송 건수, 속도 등)

### Consumer 로그 (Spring Boot)
```bash
docker logs -f stockflow-realtime
```
- 메시지 수신 로그 확인
- 처리 로그 확인

---

## 3. Kafka에서 직접 메시지 확인

### market.binance.tick 토픽 메시지 확인
```bash
cd /home/capstone01/capstone/backend/infra
docker exec -it stockflow-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market.binance.tick \
  --from-beginning \
  --max-messages 5
```

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
  --topic market.binance.tick
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
