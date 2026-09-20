package com.stockflow.realtime.config;

import com.stockflow.core.dto.NormalizedTradeDTO;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer 설정
 * 
 * 주요 설정:
 * - 수동 Offset 커밋 (enable-auto-commit: false)
 * - JSON 역직렬화
 * - 에러 처리
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.auto-offset-reset:latest}")
    private String autoOffsetReset;

    @Value("${spring.kafka.consumer.max-poll-records:100}")
    private int maxPollRecords;

    @Value("${spring.kafka.consumer.fetch-min-size:1}")
    private int fetchMinSize;

    @Value("${spring.kafka.consumer.fetch-max-wait:500}")
    private int fetchMaxWait;

    @Value("${spring.kafka.consumer.concurrency:4}")
    private int concurrency;

    @Value("${spring.kafka.consumer.properties.max.poll.interval.ms:300000}")
    private int maxPollIntervalMs;

    @Value("${spring.kafka.consumer.session-timeout-ms:30000}")
    private int sessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat-interval-ms:10000}")
    private int heartbeatIntervalMs;

    @Value("${spring.kafka.consumer.max-partition-fetch-bytes:1048576}")
    private int maxPartitionFetchBytes;

    // StorageConsumer(배치) 컨테이너 레벨 재시도 — DLQ 전송은 StorageService.saveBatch 내부에서
    // 이미 처리하므로 여기서는 recoverer 없이 바운드된 재시도 횟수만 둔다.
    // 미설정 시 Spring Kafka 기본값(FixedBackOff(0, 9) — 지연 없이 9회 재시도)이 적용되는데,
    // 지연 없이 즉시 재시도하면 실패마다 DLQ에 중복 전송이 몰아서 발생한다.
    @Value("${storage.consumer.error.max-retries:2}")
    private long storageConsumerErrorMaxRetries;

    @Value("${storage.consumer.error.backoff-ms:2000}")
    private long storageConsumerErrorBackoffMs;

    /**
     * Consumer Factory 설정
     *
     * 수동 커밋 모드로 설정하여 메시지 처리 성공 후에만 커밋
     * Micrometer를 통해 Kafka Consumer 메트릭을 Prometheus에 노출
     */
    @Bean
    public ConsumerFactory<String, NormalizedTradeDTO> consumerFactory(MeterRegistry meterRegistry) {
        Map<String, Object> props = new HashMap<>();

        // 기본 설정
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // 수동 커밋 설정
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Consumer Group 설정 (각 Consumer에서 개별 설정)
        // props.put(ConsumerConfig.GROUP_ID_CONFIG, "realtime-group");

        // 오프셋 리셋 정책 (spring.kafka.consumer.auto-offset-reset 로 설정, 장애 복구 시 운영자가 조정)
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        // 배치 처리 설정 (Storage Consumer용)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinSize);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);

        // 세션 타임아웃 설정
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);

        // poll 간격 설정 (리밸런싱 방지)
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        // 메모리 최적화
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);

        // JSON 역직렬화 설정
        JsonDeserializer<NormalizedTradeDTO> deserializer = new JsonDeserializer<>(NormalizedTradeDTO.class);
        deserializer.setUseTypeHeaders(false);
        deserializer.addTrustedPackages("*");

        DefaultKafkaConsumerFactory<String, NormalizedTradeDTO> factory =
            new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);

        // Micrometer 메트릭 바인딩 (Prometheus 노출)
        factory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        return factory;
    }

    /**
     * Kafka Listener Container Factory (단일 메시지 처리용)
     *
     * RealtimeConsumer에서 사용
     * 수동 커밋 모드로 설정
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> kafkaListenerContainerFactory(
            ConsumerFactory<String, NormalizedTradeDTO> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // 수동 커밋 모드 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 동시성 설정 (파티션 수와 동일하게)
        factory.setConcurrency(concurrency);

        return factory;
    }

    /**
     * Kafka Listener Container Factory (배치 처리용)
     *
     * StorageConsumer에서 사용
     * 배치 모드로 여러 메시지를 한 번에 처리
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, NormalizedTradeDTO> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // 배치 모드 설정
        factory.setBatchListener(true);

        // 수동 커밋 모드 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 동시성 설정
        factory.setConcurrency(concurrency);

        // 바운드된 재시도 후 스킵 (recoverer 미지정 — DLQ는 StorageService.saveBatch가 이미 처리).
        // 재시도가 소진되면 DefaultErrorHandler가 로깅 후 다음 배치로 넘어가 컨슈머가
        // 하나의 배치에 영원히 멈추지 않도록 한다.
        factory.setCommonErrorHandler(new DefaultErrorHandler(
            new FixedBackOff(storageConsumerErrorBackoffMs, storageConsumerErrorMaxRetries)));

        return factory;
    }
}
