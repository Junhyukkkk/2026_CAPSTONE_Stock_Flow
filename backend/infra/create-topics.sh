#!/bin/bash
KAFKA=localhost:9092

echo "Creating Kafka topics..."

kafka-topics --create --bootstrap-server $KAFKA \
  --topic market.normalized --partitions 12 \
  --replication-factor 1 --config retention.ms=14400000

kafka-topics --create --bootstrap-server $KAFKA \
  --topic market.dlq --partitions 3 \
  --replication-factor 1 --config retention.ms=604800000

echo "Topics created:"
kafka-topics --list --bootstrap-server $KAFKA
