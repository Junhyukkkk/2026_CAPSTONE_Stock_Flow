package com.stockflow.realtime.config;

import com.stockflow.core.dto.DLQMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * DLQ 전송을 위한 Kafka Producer 설정
 * 
 * DLQ 메시지 전송 전용 Producer
 */
@Configuration
public class KafkaDLQConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * DLQ 메시지 전송용 Producer Factory
     */
    @Bean
    public ProducerFactory<String, DLQMessage> dlqProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        
        // 신뢰성 설정
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 모든 복제본 확인
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1); // 순서 보장
        
        // 압축 설정
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * DLQ 메시지 전송용 KafkaTemplate
     */
    @Bean
    public KafkaTemplate<String, DLQMessage> dlqKafkaTemplate() {
        return new KafkaTemplate<>(dlqProducerFactory());
    }
}
