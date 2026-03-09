#!/bin/bash
KAFKA_CONTAINER=stockflow-kafka
KAFKA_BOOTSTRAP=localhost:9092

echo "Creating Kafka topics..."

# 데이터 수집 토픽 (Producer에서 사용)
docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.binance.tick --partitions 6 \
  --replication-factor 1 --config retention.ms=14400000 \
  --if-not-exists

docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.alpaca.tick --partitions 12 \
  --replication-factor 1 --config retention.ms=14400000 \
  --if-not-exists

# 정규화된 통합 토픽 (Consumer에서 사용)
docker exec $KAFKA_CONTAINER kafka-topics --create --bootstrap-server $KAFKA_BOOTSTRAP \
  --topic market.normalized --partitions 12 \
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
