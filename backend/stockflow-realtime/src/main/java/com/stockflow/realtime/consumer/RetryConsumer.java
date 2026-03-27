package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.dlq.DLQService;
import com.stockflow.realtime.retry.RetryTopicService;
import com.stockflow.realtime.service.RedisPriceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Retry 토픽 Consumer
 *
 * 실패한 메시지를 재처리하는 전용 Consumer
 * - 5초 대기 후 재처리 (poll 간격 내에서 처리)
 * - retry-count >= 3이면 DLQ로 전송
 *
 * test 프로필에서만 활성화
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "retry.mode", havingValue = "async")
@RequiredArgsConstructor
public class RetryConsumer {

    private final RedisPriceService redisPriceService;
    private final DLQService dlqService;
    private final RetryTopicService retryTopicService;
    private final PerformanceMetrics performanceMetrics;

    @Value("${kafka.topic.normalized:market.normalized.test}")
    private String originalTopic;

    @Value("${retry.delay-ms:5000}")
    private long retryDelayMs;

    @Value("${retry.max-retries:3}")
    private int maxRetries;

    private static final String CONSUMER_GROUP = "retry-test-group";

    @KafkaListener(
        topics = "${kafka.topic.retry:market.retry.test}",
        groupId = "${kafka.consumer.group.retry:retry-test-group}",
        containerFactory = "retryKafkaListenerContainerFactory"
    )
    public void consumeRetryMessage(
            @Payload NormalizedTradeDTO trade,
            Acknowledgment acknowledgment,
            @Headers Map<String, Object> headers,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        // 헤더에서 retry-count 추출
        int retryCount = extractRetryCount(headers);

        log.info("Received retry message: symbol={}, retryCount={}, partition={}, offset={}",
            trade.getSymbol(), retryCount, partition, offset);

        // 최대 재시도 횟수 초과 시 DLQ로 전송
        if (retryCount >= maxRetries) {
            log.warn("Max retries exceeded, sending to DLQ: symbol={}, retryCount={}",
                trade.getSymbol(), retryCount);

            dlqService.sendToDLQ(
                originalTopic,
                partition,
                offset,
                trade,
                new RuntimeException("Max retries exceeded: " + retryCount),
                CONSUMER_GROUP,
                retryCount
            );

            acknowledgment.acknowledge();
            performanceMetrics.recordFailure();
            return;
        }

        // 재시도 전 대기 (5초) - poll 간격(30초) 내에서 처리 가능
        try {
            log.info("Waiting {}ms before retry: symbol={}, retryCount={}",
                retryDelayMs, trade.getSymbol(), retryCount);
            Thread.sleep(retryDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Retry delay interrupted: symbol={}", trade.getSymbol());
        }

        // 재처리 시도
        try {
            long startTime = System.currentTimeMillis();

            redisPriceService.processRealtimeTrade(trade);

            long processingTime = System.currentTimeMillis() - startTime;

            // 성공
            acknowledgment.acknowledge();
            performanceMetrics.recordSuccessWithLatency(trade.getTimestamp());
            performanceMetrics.recordProcessingTime(processingTime);

            log.info("Retry successful: symbol={}, retryCount={}, processingTime={}ms",
                trade.getSymbol(), retryCount, processingTime);

        } catch (Exception e) {
            log.warn("Retry failed: symbol={}, retryCount={}, error={}",
                trade.getSymbol(), retryCount, e.getMessage());

            // 다시 retry 토픽으로 전송 (retryCount 증가)
            retryTopicService.sendToRetryTopic(trade, retryCount + 1, e);

            // 현재 메시지는 커밋 (다음 retry 메시지가 처리)
            acknowledgment.acknowledge();
        }
    }

    private int extractRetryCount(Map<String, Object> headers) {
        Object retryCountHeader = headers.get(RetryTopicService.HEADER_RETRY_COUNT);

        if (retryCountHeader == null) {
            return 1;
        }

        if (retryCountHeader instanceof byte[]) {
            String value = new String((byte[]) retryCountHeader, StandardCharsets.UTF_8);
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return 1;
            }
        }

        return 1;
    }
}
