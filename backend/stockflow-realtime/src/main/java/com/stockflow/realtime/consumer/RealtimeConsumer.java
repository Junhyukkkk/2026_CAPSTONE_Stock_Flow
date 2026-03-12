package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.performance.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessor;
import com.stockflow.realtime.transaction.RealtimeTransactionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * 실시간 데이터 Consumer
 * 
 * 역할:
 * - 정규화된 거래 데이터를 Redis에 저장
 * - 실시간 데이터 제공을 위한 낮은 지연시간 처리
 * 
 * Consumer Group: realtime-group
 * Topic: market.normalized
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RealtimeConsumer {

    private final RetryableProcessor retryableProcessor;
    private final PerformanceMetrics performanceMetrics;
    private final RealtimeTransactionManager transactionManager;

    @Value("${spring.kafka.consumer.group.realtime:realtime-group}")
    private String consumerGroup;

    // TODO: Redis 서비스 주입 (다음 단계에서 구현)
    // private final RedisService redisService;

    /**
     * 실시간 거래 데이터 수신 및 처리
     * 
     * @param trade 정규화된 거래 데이터
     * @param acknowledgment 수동 커밋을 위한 acknowledgment
     * @param partition 파티션 번호
     * @param offset 오프셋
     */
    @KafkaListener(
        topics = "${kafka.topic.normalized:market.normalized}",
        groupId = "${kafka.consumer.group.realtime:realtime-group}",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRealtimeTrade(
            @Payload NormalizedTradeDTO trade,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        log.debug("Received trade: symbol={}, price={}, partition={}, offset={}", 
            trade.getSymbol(), trade.getPrice(), partition, offset);

        long startTime = System.currentTimeMillis();

        // 트랜잭션 및 재시도 로직 포함하여 처리
        boolean success = retryableProcessor.processWithRetry(
            trade,
            (t) -> {
                // 트랜잭션으로 처리 (Idempotency 포함)
                transactionManager.processWithTransaction(t, this::processTrade);
            },
            consumerGroup,
            partition,
            offset
        );

        long processingTime = System.currentTimeMillis() - startTime;

        if (success) {
            // 처리 성공 시 커밋
            acknowledgment.acknowledge();
            performanceMetrics.recordSuccess();
            performanceMetrics.recordProcessingTime(processingTime);
            log.debug("Successfully processed trade: symbol={}, offset={}, processingTime={}ms", 
                trade.getSymbol(), offset, processingTime);
        } else {
            // 처리 실패 시 커밋하지 않음 (재처리 가능)
            // DLQ로 전송은 RetryableProcessor에서 처리됨
            performanceMetrics.recordFailure();
            log.warn("Failed to process trade: symbol={}, partition={}, offset={}", 
                trade.getSymbol(), partition, offset);
        }
    }

    /**
     * 실제 거래 데이터 처리 로직
     * 
     * @param trade 처리할 거래 데이터
     */
    private void processTrade(NormalizedTradeDTO trade) {
        // TODO: Redis에 저장 (다음 단계에서 구현)
        // redisService.saveRealtimeTrade(trade);
        
        log.trace("Processing trade: symbol={}, price={}", 
            trade.getSymbol(), trade.getPrice());
    }
}
