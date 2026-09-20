#!/bin/bash
# 주의: docker-compose.yml 의 `kafka-setup` 1회성 서비스가 `docker compose up` 때마다
# 이미 이 토픽들을 (동일한 파티션/보관 설정으로) 자동 생성한다. 이 스크립트는 그 로직을
# 그대로 복제한 수동 실행용이며, 평소에는 실행할 필요가 없다 — kafka-setup 이 유일한
# source of truth 이므로 토픽 구성을 바꿀 때는 docker-compose.yml 을 먼저 고치고
# 이 스크립트도 맞춰서 갱신할 것.
# 용도: kafka-setup 이 완주하지 못했거나(기동 타이밍 등) 토픽을 수동으로 재생성해야 할 때만.
KAFKA_CONTAINER=stockflow-kafka
KAFKA_BOOTSTRAP=localhost:9092

echo "Creating Kafka topics (kafka-setup 과 동일한 구성)..."

# 정규화된 통합 토픽 (수집기 → Consumer, KAFKA_TOPIC_NAME=market.normalized)
docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.normalized --partitions 12 \
  --replication-factor 1 --config retention.ms=14400000 \
  --if-not-exists

# 재시도 토픽 (RetryConsumer)
docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.retry --partitions 6 \
  --replication-factor 1 --config retention.ms=14400000 \
  --if-not-exists

# Dead Letter Queue (실패한 메시지)
docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.dlq --partitions 3 \
  --replication-factor 1 --config retention.ms=604800000 \
  --if-not-exists

echo ""
echo "✅ Topics created:"
docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server $KAFKA_BOOTSTRAP
