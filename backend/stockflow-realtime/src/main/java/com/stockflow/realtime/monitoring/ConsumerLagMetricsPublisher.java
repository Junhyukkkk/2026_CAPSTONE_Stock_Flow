package com.stockflow.realtime.monitoring;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Consumer Lag / 유실 위험 지표 상시 발행
 *
 * 기존 ConsumerLagMonitor 는 API 호출 시에만 lag 을 계산해 Prometheus 에 상시 노출되지 않았다.
 * 이 컴포넌트는 주기적으로 그룹·파티션별 지표를 계산해 Micrometer 게이지로 노출한다.
 *
 * 노출 지표:
 * - stockflow_consumer_lag{group,partition}
 *     밀린 메시지 수 (endOffset - committedOffset). 클수록 처리가 도착을 못 따라감.
 * - stockflow_consumer_retention_margin{group,partition}
 *     커밋 위치가 삭제 경계(logStart)에서 얼마나 떨어져 있나 (committedOffset - beginningOffset).
 *     이 값이 0에 가까워지면 다음 retention 삭제 때 미처리 데이터가 유실된다 = 유실 임박 신호.
 *
 * 경보는 이 두 지표를 기준으로 Prometheus rule 에서 정의한다.
 */
@Slf4j
@Component
public class ConsumerLagMetricsPublisher {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    @Value("${spring.kafka.consumer.group.realtime:realtime-group}")
    private String realtimeGroup;

    @Value("${spring.kafka.consumer.group.storage:storage-group}")
    private String storageGroup;

    private final MultiGauge lagGauge;
    private final MultiGauge retentionMarginGauge;

    public ConsumerLagMetricsPublisher(MeterRegistry registry) {
        this.lagGauge = MultiGauge.builder("stockflow.consumer.lag")
                .description("Unprocessed messages per group/partition (endOffset - committedOffset)")
                .register(registry);
        this.retentionMarginGauge = MultiGauge.builder("stockflow.consumer.retention.margin")
                .description("Offsets between committed position and deletion frontier; near 0 = data loss imminent")
                .register(registry);
    }

    /** 15초마다 두 Consumer Group 의 lag / retention margin 을 갱신한다. */
    @Scheduled(fixedDelayString = "${monitoring.lag.interval-ms:15000}")
    public void publish() {
        List<MultiGauge.Row<?>> lagRows = new ArrayList<>();
        List<MultiGauge.Row<?>> marginRows = new ArrayList<>();

        for (String group : List.of(realtimeGroup, storageGroup)) {
            collect(group, lagRows, marginRows);
        }

        lagGauge.register(lagRows, true);
        retentionMarginGauge.register(marginRows, true);
    }

    private void collect(String group,
                         List<MultiGauge.Row<?>> lagRows,
                         List<MultiGauge.Row<?>> marginRows) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            if (consumer.partitionsFor(topicName) == null) {
                return;
            }
            Set<TopicPartition> partitions = consumer.partitionsFor(topicName).stream()
                    .map(pi -> new TopicPartition(topicName, pi.partition()))
                    .collect(Collectors.toSet());

            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(partitions);

            for (TopicPartition tp : partitions) {
                long end = endOffsets.getOrDefault(tp, 0L);
                long begin = beginningOffsets.getOrDefault(tp, 0L);
                OffsetAndMetadata c = committed.get(tp);
                long commit = c != null ? c.offset() : begin;   // 커밋 없으면 시작 지점으로 간주

                long lag = Math.max(0, end - commit);
                long margin = Math.max(0, commit - begin);

                Tags tags = Tags.of("group", group, "partition", String.valueOf(tp.partition()));
                lagRows.add(MultiGauge.Row.of(tags, lag));
                marginRows.add(MultiGauge.Row.of(tags, margin));
            }
        } catch (Exception e) {
            log.warn("Consumer lag 지표 수집 실패: group={}", group, e);
        }
    }
}
