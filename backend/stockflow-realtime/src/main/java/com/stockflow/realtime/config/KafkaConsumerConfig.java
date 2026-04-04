package com.stockflow.realtime.config;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

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

    /**
     * Consumer Factory 설정
     * 
     * 수동 커밋 모드로 설정하여 메시지 처리 성공 후에만 커밋
     */
    @Bean
    public ConsumerFactory<String, NormalizedTradeDTO> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        // 기본 설정
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        // 수동 커밋 설정
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        
        // Consumer Group 설정 (각 Consumer에서 개별 설정)
        // props.put(ConsumerConfig.GROUP_ID_CONFIG, "realtime-group");
        
        // 오프셋 리셋 정책
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
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
        
        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            deserializer
        );
    }

    /**
     * Kafka Listener Container Factory (단일 메시지 처리용)
     * 
     * RealtimeConsumer에서 사용
     * 수동 커밋 모드로 설정
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
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
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> batchKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        
        factory.setConsumerFactory(consumerFactory());
        
        // 배치 모드 설정
        factory.setBatchListener(true);
        
        // 수동 커밋 모드 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        
        // 동시성 설정
        factory.setConcurrency(concurrency);

        return factory;
    }
}
