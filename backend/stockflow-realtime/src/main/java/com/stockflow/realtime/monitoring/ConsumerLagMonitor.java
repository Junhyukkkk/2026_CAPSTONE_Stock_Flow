package com.stockflow.realtime.monitoring;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Consumer Lag 모니터링
 * 
 * Consumer가 처리 못한 메시지 수 추적
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ConsumerLagMonitor {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    /**
     * Consumer Group의 Lag 조회
     * 
     * @param groupId Consumer Group ID
     * @return 파티션별 Lag 맵
     */
    public Map<Integer, Long> getConsumerLag(String groupId) {
        Map<Integer, Long> lagMap = new HashMap<>();
        
        try {
            // Consumer 생성 (Lag 조회용)
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            
            try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
                // 토픽 파티션 정보 조회
                Set<TopicPartition> partitions = consumer.partitionsFor(topicName).stream()
                    .map(pi -> new TopicPartition(topicName, pi.partition()))
                    .collect(Collectors.toSet());
                
                consumer.assign(partitions);
                
                // 각 파티션의 Lag 계산
                for (TopicPartition partition : partitions) {
                    long endOffset = consumer.endOffsets(Collections.singleton(partition))
                        .get(partition);
                    long committedOffset = consumer.committed(partition) != null ?
                        consumer.committed(partition).offset() : 0;
                    
                    long lag = endOffset - committedOffset;
                    lagMap.put(partition.partition(), lag);
                }
            }
            
        } catch (Exception e) {
            log.error("Failed to get consumer lag: groupId={}", groupId, e);
        }
        
        return lagMap;
    }

    /**
     * 전체 Lag 합계
     */
    public long getTotalLag(String groupId) {
        return getConsumerLag(groupId).values().stream()
            .mapToLong(Long::longValue)
            .sum();
    }
}
