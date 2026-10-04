package com.stockflow.realtime.config;

import com.stockflow.core.dto.NormalizedTradeDTO;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 저장 컨슈머 전용 ConsumerFactory 분리 검증.
 * 기본값에서는 realtime 팩토리와 동일하고, KAFKA_STORAGE_* 는 storage 팩토리에만 반영되어야 한다.
 */
class KafkaConsumerConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withUserConfiguration(KafkaConsumerConfig.class)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:9092");

    private static Map<String, Object> props(ConsumerFactory<String, NormalizedTradeDTO> factory) {
        return factory.getConfigurationProperties();
    }

    @SuppressWarnings("unchecked")
    private static ConsumerFactory<String, NormalizedTradeDTO> factoryOf(
            org.springframework.context.ApplicationContext ctx, String listenerFactoryBean) {
        return (ConsumerFactory<String, NormalizedTradeDTO>) ((ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO>)
            ctx.getBean(listenerFactoryBean)).getConsumerFactory();
    }

    @Test
    void defaultsMatchSharedValues() {
        runner.run(ctx -> {
            Map<String, Object> realtime = props(factoryOf(ctx, "kafkaListenerContainerFactory"));
            Map<String, Object> storage = props(factoryOf(ctx, "batchKafkaListenerContainerFactory"));

            assertThat(realtime.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
            assertThat(realtime.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)).isEqualTo(1);
            assertThat(realtime.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)).isEqualTo(500);
            assertThat(storage).isEqualTo(realtime);
        });
    }

    @Test
    void storageFactoryFollowsSharedOverridesWhenNoStorageEnvIsSet() {
        runner.withPropertyValues(
            "spring.kafka.consumer.max-poll-records=250",
            "spring.kafka.consumer.fetch-min-size=64",
            "spring.kafka.consumer.fetch-max-wait=200").run(ctx -> {
            Map<String, Object> storage = props(factoryOf(ctx, "batchKafkaListenerContainerFactory"));

            assertThat(storage.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(250);
            assertThat(storage.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)).isEqualTo(64);
            assertThat(storage.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)).isEqualTo(200);
        });
    }

    @Test
    void storageOverridesApplyOnlyToStorageFactory() {
        runner.withPropertyValues(
            "KAFKA_STORAGE_MAX_POLL_RECORDS=500",
            "KAFKA_STORAGE_FETCH_MIN_BYTES=4096",
            "KAFKA_STORAGE_FETCH_MAX_WAIT_MS=100").run(ctx -> {
            Map<String, Object> realtime = props(factoryOf(ctx, "kafkaListenerContainerFactory"));
            Map<String, Object> storage = props(factoryOf(ctx, "batchKafkaListenerContainerFactory"));

            assertThat(storage.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(500);
            assertThat(storage.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)).isEqualTo(4096);
            assertThat(storage.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)).isEqualTo(100);

            assertThat(realtime.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(100);
            assertThat(realtime.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG)).isEqualTo(1);
            assertThat(realtime.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)).isEqualTo(500);

            // 나머지 설정은 동일
            Map<String, Object> storageRest = new java.util.HashMap<>(storage);
            Map<String, Object> realtimeRest = new java.util.HashMap<>(realtime);
            for (String k : List.of(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG)) {
                storageRest.remove(k);
                realtimeRest.remove(k);
            }
            assertThat(storageRest).isEqualTo(realtimeRest);
        });
    }

    @Test
    void bothFactoriesAreSeparateBeansWithMicrometerListener() {
        runner.run(ctx -> {
            ConsumerFactory<String, NormalizedTradeDTO> realtime = factoryOf(ctx, "kafkaListenerContainerFactory");
            ConsumerFactory<String, NormalizedTradeDTO> storage = factoryOf(ctx, "batchKafkaListenerContainerFactory");

            assertThat(realtime).isSameAs(ctx.getBean("consumerFactory"));
            assertThat(storage).isSameAs(ctx.getBean("storageConsumerFactory"));
            assertThat(storage).isNotSameAs(realtime);

            for (ConsumerFactory<String, NormalizedTradeDTO> f : List.of(realtime, storage)) {
                List<?> listeners = (List<?>) ReflectionTestUtils.getField(f, "listeners");
                assertThat(listeners).hasAtLeastOneElementOfType(MicrometerConsumerListener.class);
                assertThat(f).isInstanceOf(DefaultKafkaConsumerFactory.class);
            }
        });
    }

    @Test
    void realtimeBatchFactoryIsBatchManualAckOnRealtimeConsumerFactory() {
        runner.run(ctx -> {
            ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> batch =
                ctx.getBean("realtimeBatchKafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);
            ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> single =
                ctx.getBean("kafkaListenerContainerFactory", ConcurrentKafkaListenerContainerFactory.class);

            assertThat(batch.getConsumerFactory()).isSameAs(ctx.getBean("consumerFactory"));
            assertThat(ReflectionTestUtils.getField(batch, "batchListener")).isEqualTo(true);
            assertThat(batch.getContainerProperties().getAckMode())
                .isEqualTo(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);

            // 기존 단건 팩토리는 배치 모드가 아니다 (건드리지 않았다)
            assertThat(ReflectionTestUtils.getField(single, "batchListener")).isNotEqualTo(true);
        });
    }
}
