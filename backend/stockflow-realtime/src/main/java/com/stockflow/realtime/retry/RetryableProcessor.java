package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.realtime.dlq.DLQService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

/**
 * 재시도 가능한 메시지 처리기 (test 프로필용)
 *
 * 비동기 Retry 토픽 방식으로 리밸런싱 방지
 * Consumer에서 사용하는 재시도 로직을 캡슐화
 */
@Slf4j
@Component
@Profile("test")
@RequiredArgsConstructor
public class RetryableProcessor implements RetryableProcessorInterface {

    private final ErrorClassifier errorClassifier;
    private final DLQService dlqService;
    private final RetryTopicService retryTopicService;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    @Value("${retry.max-retries:3}")
    private int maxRetries;

    /**
     * 단일 메시지 처리 (비동기 재시도 - Retry 토픽 사용)
     *
     * 동기 재시도 대신 실패 시 retry 토픽으로 전송하여
     * poll 간격 초과로 인한 리밸런싱을 방지
     *
     * @param trade 처리할 거래 데이터
     * @param processor 실제 처리 로직
     * @param consumerGroup Consumer Group 이름
     * @param partition 파티션 번호
     * @param offset 오프셋
     * @return 처리 성공 여부 (retry 토픽 전송도 성공으로 처리하여 커밋)
     */
    public boolean processWithRetry(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor,
            String consumerGroup,
            int partition,
            long offset) {

        return processWithRetry(trade, processor, consumerGroup, partition, offset, 0);
    }

    /**
     * 단일 메시지 처리 (재시도 횟수 포함)
     */
    public boolean processWithRetry(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor,
            String consumerGroup,
            int partition,
            long offset,
            int retryCount) {

        try {
            processor.accept(trade);
            return true;

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);

            // 재시도 불가능한 에러는 즉시 DLQ로 전송
            if (!errorClassifier.isRetryable(errorType)) {
                log.error("Non-retryable error: symbol={}, partition={}, offset={}, errorType={}",
                    trade.getSymbol(), partition, offset, errorType);

                dlqService.sendToDLQ(
                    topicName, partition, offset, trade, e, consumerGroup, retryCount
                );
                return true;  // DLQ 전송 후 커밋
            }

            // 최대 재시도 횟수 초과 시 DLQ로 전송
            if (retryCount >= maxRetries) {
                log.error("Max retries exceeded: symbol={}, partition={}, offset={}, retryCount={}",
                    trade.getSymbol(), partition, offset, retryCount);

                dlqService.sendToDLQ(
                    topicName, partition, offset, trade, e, consumerGroup, retryCount
                );
                return true;  // DLQ 전송 후 커밋
            }

            // 재시도 가능한 에러 → Retry 토픽으로 전송 (비동기)
            log.warn("Sending to retry topic: symbol={}, partition={}, offset={}, retryCount={}, error={}",
                trade.getSymbol(), partition, offset, retryCount, e.getMessage());

            retryTopicService.sendToRetryTopic(trade, retryCount + 1, e);
            return true;  // Retry 토픽 전송 후 커밋 (원본 메시지는 처리 완료)
        }
    }

    /**
     * 배치 메시지 처리 (비동기 재시도 - Retry 토픽 사용)
     *
     * @param trades 처리할 거래 데이터 리스트
     * @param processor 실제 처리 로직
     * @param consumerGroup Consumer Group 이름
     * @return 처리 성공 여부
     */
    public boolean processBatchWithRetry(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor,
            String consumerGroup) {

        return processBatchWithRetry(trades, processor, consumerGroup, 0);
    }

    /**
     * 배치 메시지 처리 (재시도 횟수 포함)
     */
    public boolean processBatchWithRetry(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor,
            String consumerGroup,
            int retryCount) {

        try {
            processor.accept(trades);
            return true;

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);

            // 재시도 불가능한 에러는 즉시 DLQ로 전송
            if (!errorClassifier.isRetryable(errorType)) {
                log.error("Non-retryable batch error: size={}, errorType={}",
                    trades.size(), errorType);

                dlqService.sendBatchToDLQ(topicName, trades, e, consumerGroup);
                return true;  // DLQ 전송 후 커밋
            }

            // 최대 재시도 횟수 초과 시 DLQ로 전송
            if (retryCount >= maxRetries) {
                log.error("Max retries exceeded for batch: size={}, retryCount={}",
                    trades.size(), retryCount);

                dlqService.sendBatchToDLQ(topicName, trades, e, consumerGroup);
                return true;  // DLQ 전송 후 커밋
            }

            // 재시도 가능한 에러 → Retry 토픽으로 전송 (비동기)
            log.warn("Sending batch to retry topic: size={}, retryCount={}, error={}",
                trades.size(), retryCount, e.getMessage());

            retryTopicService.sendBatchToRetryTopic(trades, retryCount + 1, e);
            return true;  // Retry 토픽 전송 후 커밋
        }
    }
}
