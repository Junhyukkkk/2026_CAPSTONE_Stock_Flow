package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.storage.StorageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 저장용 Kafka Consumer
 *
 * Kafka에서 메시지를 수신하고 StorageService에 위임
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StorageConsumer {

    private final StorageService storageService;
    private final PerformanceMetrics performanceMetrics;

    @Value("${spring.kafka.consumer.group.storage:storage-group}")
    private String consumerGroup;

    @KafkaListener(
        topics = "${spring.kafka.topic.normalized:market.normalized}",
        groupId = "${spring.kafka.consumer.group.storage:storage-group}",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeStorageTrades(
            @Payload List<NormalizedTradeDTO> trades,
            Acknowledgment acknowledgment) {

        if (trades == null || trades.isEmpty()) {
            acknowledgment.acknowledge();
            return;
        }

        log.debug("Received batch: size={}", trades.size());
        long startTime = System.currentTimeMillis();

        try {
            boolean success = storageService.saveBatch(trades, consumerGroup);

            if (!success) {
                // 실패 메트릭은 아래 catch 블록에서 일괄 기록 (이중 집계 방지)
                throw new RuntimeException("Batch processing failed");
            }

            acknowledgment.acknowledge();

            long processingTime = System.currentTimeMillis() - startTime;
            performanceMetrics.recordSuccess();
            performanceMetrics.recordProcessingTime(processingTime);

            log.debug("Processed batch: size={}, time={}ms", trades.size(), processingTime);

        } catch (Exception e) {
            performanceMetrics.recordFailure();
            throw e;
        }
    }
}
