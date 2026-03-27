package com.stockflow.realtime.test;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Retry Consumer 전용 Kafka 설정
 *
 * test 프로필에서만 활성화
 */
@Configuration
@Profile("test")
public class TestRetryKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.properties.max.poll.interval.ms:30000}")
    private int maxPollIntervalMs;

    @Bean
    public ConsumerFactory<String, NormalizedTradeDTO> retryConsumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // 수동 커밋
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // max.poll.interval.ms 설정 (리밸런싱 테스트용)
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        // 한 번에 하나씩 처리 (retry는 개별 처리)
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

        // 세션 타임아웃
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        JsonDeserializer<NormalizedTradeDTO> deserializer = new JsonDeserializer<>(NormalizedTradeDTO.class);
        deserializer.setUseTypeHeaders(false);
        deserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> retryKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
            new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(retryConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Retry Consumer는 단일 스레드로 순차 처리
        factory.setConcurrency(1);

        return factory;
    }
}
