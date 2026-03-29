package com.stockflow.realtime.consumer;

import com.stockflow.core.util.BatchProcessor;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessorInterface;
import com.stockflow.realtime.storage.MarketTickBulkWriter;
import com.stockflow.realtime.transaction.RealtimeTransactionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 저장용 데이터 Consumer
 * 
 * 역할:
 * - 정규화된 거래 데이터를 PostgreSQL (TimescaleDB)에 저장
 * - 배치 처리로 성능 최적화
 * - 분석 및 백테스팅용 데이터 제공
 * 
 * Consumer Group: storage-group
 * Topic: market.normalized
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class StorageConsumer {

    private final RetryableProcessorInterface retryableProcessor;
    private final BatchProcessor batchProcessor;
    private final PerformanceMetrics performanceMetrics;
    private final RealtimeTransactionManager transactionManager;
    private final MarketTickBulkWriter marketTickBulkWriter;

    @Value("${spring.kafka.consumer.group.storage:storage-group}")
    private String consumerGroup;

    /**
     * 저장용 거래 데이터 수신 및 배치 처리
     * 
     * 배치 처리 모드로 여러 메시지를 한 번에 처리하여 성능 최적화
     * 
     * @param trades 정규화된 거래 데이터 리스트 (배치)
     * @param acknowledgment 수동 커밋을 위한 acknowledgment
     */
    @KafkaListener(
        topics = "${spring.kafka.topic.normalized:market.normalized}",
        groupId = "${spring.kafka.consumer.group.storage:storage-group}",
        containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void consumeStorageTrades(
            @Payload List<NormalizedTradeDTO> trades,
            Acknowledgment acknowledgment) {
        
        log.debug("Received batch: size={}", trades.size());

        long startTime = System.currentTimeMillis();

        try {
            // 배치 처리기로 최적화된 배치 처리
            batchProcessor.processBatch(trades, (batch) -> {
                // 재시도 로직 포함하여 배치 처리
                boolean success = retryableProcessor.processBatchWithRetry(
                    batch,
                    (b) -> {
                        // 트랜잭션으로 배치 처리 (Idempotency 포함)
                        transactionManager.processBatchWithTransaction(b, this::processBatch);
                    },
                    consumerGroup
                );

                if (!success) {
                    throw new RuntimeException("Batch processing failed");
                }
            });

            // 배치 전체 성공 후 커밋
            acknowledgment.acknowledge();
            
            long processingTime = System.currentTimeMillis() - startTime;
            performanceMetrics.recordSuccess();
            performanceMetrics.recordProcessingTime(processingTime);
            
            log.debug("Successfully processed batch: size={}, processingTime={}ms", 
                trades.size(), processingTime);
                
        } catch (Exception e) {
            performanceMetrics.recordFailure();
            throw e;
        }
    }

    /**
     * 실제 배치 데이터 처리 로직
     * 
     * @param trades 처리할 거래 데이터 리스트
     */
    private void processBatch(List<NormalizedTradeDTO> trades) {
        marketTickBulkWriter.insertBatch(trades);
        log.trace("Processing batch: size={}", trades.size());
    }
}
