package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.core.retry.RetryPolicy;
import com.stockflow.core.retry.RetryService;
import com.stockflow.realtime.dlq.DLQService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

/**
 * 재시도 가능한 메시지 처리기 (동기 방식)
 *
 * RETRY_MODE=sync 일 때 활성화 (기본값)
 * 동기 재시도로 리밸런싱 발생 가능
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "retry.mode", havingValue = "sync", matchIfMissing = true)
@RequiredArgsConstructor
public class RetryableProcessorDefault implements RetryableProcessorInterface {

    private final RetryService retryService;
    private final ErrorClassifier errorClassifier;
    private final DLQService dlqService;
    private final RetryPolicy retryPolicy;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    public boolean processWithRetry(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor,
            String consumerGroup,
            int partition,
            long offset) {

        try {
            try {
                processor.accept(trade);
                return true;
            } catch (Exception e) {
                ErrorType errorType = errorClassifier.classify(e);

                if (!errorClassifier.isRetryable(errorType)) {
                    throw new NonRetryableException(e, errorType);
                }

                retryService.executeWithRetry(
                    () -> {
                        processor.accept(trade);
                        return null;
                    },
                    errorType,
                    retryPolicy
                );

                return true;
            }

        } catch (NonRetryableException e) {
            log.error("Non-retryable error: symbol={}, partition={}, offset={}, errorType={}",
                trade.getSymbol(), partition, offset, e.getErrorType());

            return sendToDlqAndAcknowledge(
                trade, partition, offset, e.getCause(), consumerGroup, 0
            );

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed after retries: symbol={}, partition={}, offset={}, errorType={}",
                trade.getSymbol(), partition, offset, errorType);

            return sendToDlqAndAcknowledge(
                trade, partition, offset, e, consumerGroup, 3
            );
        }
    }

    /**
     * DLQ로 전송하고, 전송 자체가 성공하면 true(커밋 허용)를 반환한다.
     *
     * 여기서 무조건 false 를 반환하면 RealtimeConsumer가 이 오프셋을 영원히 커밋하지 않아
     * 컨슈머가 해당 오프셋에 멈춘다. 이후 retention으로 로그 세그먼트가 삭제되면
     * auto-offset-reset이 발동해 그 사이 메시지가 전부 유실된다.
     * DLQ 전송 자체가 실패한 경우에만 커밋을 보류(false)한다.
     */
    private boolean sendToDlqAndAcknowledge(
            NormalizedTradeDTO trade,
            int partition,
            long offset,
            Throwable cause,
            String consumerGroup,
            int retryCount) {
        try {
            dlqService.sendToDLQ(topicName, partition, offset, trade, cause, consumerGroup, retryCount);
            return true; // DLQ 전송 후 커밋
        } catch (Exception dlqEx) {
            log.error("Failed to send message to DLQ, withholding ack: symbol={}, partition={}, offset={}",
                trade.getSymbol(), partition, offset, dlqEx);
            return false;
        }
    }

    public boolean processBatchWithRetry(
            List<NormalizedTradeDTO> trades,
            Consumer<List<NormalizedTradeDTO>> processor,
            String consumerGroup) {

        try {
            try {
                processor.accept(trades);
                return true;
            } catch (Exception e) {
                ErrorType errorType = errorClassifier.classify(e);

                if (!errorClassifier.isRetryable(errorType)) {
                    throw new NonRetryableException(e, errorType);
                }

                retryService.executeWithRetry(
                    () -> {
                        processor.accept(trades);
                        return null;
                    },
                    errorType,
                    retryPolicy
                );

                return true;
            }

        } catch (NonRetryableException e) {
            log.error("Non-retryable batch error: size={}, errorType={}",
                trades.size(), e.getErrorType());

            return sendBatchToDlqAndAcknowledge(trades, e.getCause(), consumerGroup);

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed batch after retries: size={}, errorType={}",
                trades.size(), errorType);

            return sendBatchToDlqAndAcknowledge(trades, e, consumerGroup);
        }
    }

    /**
     * 배치를 DLQ로 전송하고, 전송 자체가 성공하면 true(커밋 허용)를 반환한다.
     * 단일 메시지 경로와 동일한 이유로 DLQ 전송 자체가 실패한 경우에만 false를 반환한다.
     */
    private boolean sendBatchToDlqAndAcknowledge(
            List<NormalizedTradeDTO> trades, Throwable cause, String consumerGroup) {
        try {
            dlqService.sendBatchToDLQ(topicName, trades, cause, consumerGroup);
            return true; // DLQ 전송 후 커밋
        } catch (Exception dlqEx) {
            log.error("Failed to send batch to DLQ, withholding ack: size={}", trades.size(), dlqEx);
            return false;
        }
    }

    private static class NonRetryableException extends RuntimeException {
        private final ErrorType errorType;

        public NonRetryableException(Throwable cause, ErrorType errorType) {
            super(cause);
            this.errorType = errorType;
        }

        public ErrorType getErrorType() {
            return errorType;
        }
    }
}
