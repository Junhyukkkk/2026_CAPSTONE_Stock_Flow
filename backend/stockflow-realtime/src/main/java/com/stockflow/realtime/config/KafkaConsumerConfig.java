package com.stockflow.realtime.config;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * 공통 Consumer 설정
     */
    private Map<String, Object> baseConsumerConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.stockflow.core.dto");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.stockflow.core.dto.NormalizedTradeDTO");
        return props;
    }

    /**
     * Realtime Consumer Factory
     * - 건건이 빠르게 처리
     * - Redis 캐싱 + Pub/Sub 용도
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO>
            realtimeKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        Map<String, Object> props = baseConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "realtime-consumer-group");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 100);  // 빠른 응답
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setConcurrency(3);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    /**
     * Storage Consumer Factory
     * - 배치로 묶어서 처리
     * - TimescaleDB Bulk Insert 용도
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO>
            storageKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, NormalizedTradeDTO> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        Map<String, Object> props = baseConsumerConfig();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "storage-consumer-group");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);  // 배치 모으기
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);

        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(props));
        factory.setConcurrency(3);
        factory.setBatchListener(true);  // 배치 리스너
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
