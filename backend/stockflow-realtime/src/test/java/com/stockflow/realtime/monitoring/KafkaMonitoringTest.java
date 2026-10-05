package com.stockflow.realtime.monitoring;

import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.health.KafkaHealthIndicator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** 임베디드 Kafka 를 상대로 한 Consumer Lag 모니터·지표 발행기·헬스 인디케이터 테스트. */
@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = {"lag.test.topic"})
class KafkaMonitoringTest {

    private static final String TOPIC = "lag.test.topic";
    /** 브로커는 클래스 안의 테스트들이 공유하므로 지금까지 발행한 총량을 추적한다. */
    private static final java.util.concurrent.atomic.AtomicLong PRODUCED = new java.util.concurrent.atomic.AtomicLong();

    @Autowired
    EmbeddedKafkaBroker broker;

    ConsumerLagMonitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new ConsumerLagMonitor();
        ReflectionTestUtils.setField(monitor, "bootstrapServers", broker.getBrokersAsString());
        ReflectionTestUtils.setField(monitor, "topicName", TOPIC);
    }

    private void produce(int count) {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker.getBrokersAsString());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "k" + i, "v" + i));
            }
            producer.flush();
        }
        PRODUCED.addAndGet(count);
    }

    private void commit(String group, long offset) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokersAsString());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition tp = new TopicPartition(TOPIC, 0);
            consumer.assign(List.of(tp));
            consumer.commitSync(Map.of(tp, new OffsetAndMetadata(offset)));
        }
    }

    @Test
    void lagMonitorReportsEndOffsetMinusCommittedOffset() {
        produce(5);

        long total = PRODUCED.get();
        assertThat(monitor.getConsumerLag("never-committed-group")).containsEntry(0, total);
        assertThat(monitor.getTotalLag("never-committed-group")).isEqualTo(total);

        commit("committed-group", 2);
        assertThat(monitor.getConsumerLag("committed-group")).containsEntry(0, total - 2);
    }

    @Test
    void lagMonitorReturnsEmptyMapWhenBrokerUnavailable() {
        ConsumerLagMonitor broken = new ConsumerLagMonitor();
        ReflectionTestUtils.setField(broken, "bootstrapServers", "");
        ReflectionTestUtils.setField(broken, "topicName", TOPIC);

        assertThat(broken.getConsumerLag("g")).isEmpty();
        assertThat(broken.getTotalLag("g")).isZero();
    }

    @Test
    void metricsPublisherExposesLagAndRetentionMarginGauges() {
        produce(4);
        commit("lag-realtime", 1);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerLagMetricsPublisher publisher = new ConsumerLagMetricsPublisher(registry);
        ReflectionTestUtils.setField(publisher, "bootstrapServers", broker.getBrokersAsString());
        ReflectionTestUtils.setField(publisher, "topicName", TOPIC);
        ReflectionTestUtils.setField(publisher, "realtimeGroup", "lag-realtime");
        ReflectionTestUtils.setField(publisher, "storageGroup", "lag-storage-uncommitted");

        publisher.publish();

        double realtimeLag = registry.get("stockflow.consumer.lag").tag("group", "lag-realtime").gauge().value();
        double storageLag = registry.get("stockflow.consumer.lag").tag("group", "lag-storage-uncommitted").gauge().value();
        assertThat(storageLag).isGreaterThanOrEqualTo(realtimeLag);
        assertThat(realtimeLag).isGreaterThanOrEqualTo(0);
        assertThat(registry.get("stockflow.consumer.retention.margin").tag("group", "lag-realtime").gauge().value())
                .isGreaterThanOrEqualTo(0);
    }

    @Test
    void metricsPublisherSurvivesCollectionFailure() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerLagMetricsPublisher publisher = new ConsumerLagMetricsPublisher(registry);
        ReflectionTestUtils.setField(publisher, "bootstrapServers", "");
        ReflectionTestUtils.setField(publisher, "topicName", TOPIC);
        ReflectionTestUtils.setField(publisher, "realtimeGroup", "g1");
        ReflectionTestUtils.setField(publisher, "storageGroup", "g2");

        publisher.publish();

        assertThat(registry.find("stockflow.consumer.lag").gauges()).isEmpty();
    }

    @Test
    void kafkaHealthIndicatorIsUpAgainstRunningBroker() {
        KafkaHealthIndicator indicator = new KafkaHealthIndicator();
        ReflectionTestUtils.setField(indicator, "bootstrapServers", broker.getBrokersAsString());

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsKeys("clusterId", "nodeCount");
    }

    @Test
    void schedulerAndControllerUseTheLagMonitor() {
        produce(2);
        PerformanceMetrics metrics = new PerformanceMetrics(new SimpleMeterRegistry());
        MetricsScheduler scheduler = new MetricsScheduler(metrics, monitor);
        ReflectionTestUtils.setField(scheduler, "realtimeGroup", "sched-realtime");
        ReflectionTestUtils.setField(scheduler, "storageGroup", "sched-storage");
        scheduler.logMetrics(); // lag > 0 경로

        commit("zero-lag-a", 100_000);
        ReflectionTestUtils.setField(scheduler, "realtimeGroup", "zero-lag-a");
        ReflectionTestUtils.setField(scheduler, "storageGroup", "zero-lag-a");
        scheduler.logMetrics(); // lag == 0 경로(음수는 합산되어도 > 0 아님)
    }
}
