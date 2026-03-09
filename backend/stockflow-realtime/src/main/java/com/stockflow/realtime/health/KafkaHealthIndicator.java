package com.stockflow.realtime.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 헬스체크
 * 
 * Kafka 클러스터 연결 상태 확인
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaHealthIndicator implements HealthIndicator {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Override
    public Health health() {
        try {
            // AdminClient로 클러스터 상태 확인
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            props.put(AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);

            try (AdminClient adminClient = AdminClient.create(props)) {
                DescribeClusterResult clusterResult = adminClient.describeCluster();
                
                String clusterId = clusterResult.clusterId().get(5, TimeUnit.SECONDS);
                int nodeCount = clusterResult.nodes().get(5, TimeUnit.SECONDS).size();
                
                return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodeCount", nodeCount)
                    .withDetail("bootstrapServers", bootstrapServers)
                    .build();
            }
            
        } catch (Exception e) {
            log.error("Kafka health check failed", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .withDetail("bootstrapServers", bootstrapServers)
                .build();
        }
    }
}
