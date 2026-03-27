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
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

/**
 * 재시도 가능한 메시지 처리기 (운영용 - 동기 방식)
 *
 * test 프로필이 아닐 때 활성화
 */
@Slf4j
@Component
@Profile("!test")
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

            dlqService.sendToDLQ(
                topicName, partition, offset, trade, e.getCause(), consumerGroup, 0
            );

            return false;

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed after retries: symbol={}, partition={}, offset={}, errorType={}",
                trade.getSymbol(), partition, offset, errorType);

            dlqService.sendToDLQ(
                topicName, partition, offset, trade, e, consumerGroup, 3
            );

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

            dlqService.sendBatchToDLQ(topicName, trades, e.getCause(), consumerGroup);

            return false;

        } catch (Exception e) {
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed batch after retries: size={}, errorType={}",
                trades.size(), errorType);

            dlqService.sendBatchToDLQ(topicName, trades, e, consumerGroup);

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
