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
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.function.Consumer;

/**
 * 재시도 가능한 메시지 처리기
 * 
 * Consumer에서 사용하는 재시도 로직을 캡슐화
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryableProcessor {

    private final RetryService retryService;
    private final ErrorClassifier errorClassifier;
    private final DLQService dlqService;
    private final RetryPolicy retryPolicy;

    @Value("${spring.kafka.topic.normalized:market.normalized}")
    private String topicName;

    /**
     * 단일 메시지 처리 (재시도 포함)
     * 
     * @param trade 처리할 거래 데이터
     * @param processor 실제 처리 로직
     * @param consumerGroup Consumer Group 이름
     * @param partition 파티션 번호
     * @param offset 오프셋
     * @return 처리 성공 여부
     */
    public boolean processWithRetry(
            NormalizedTradeDTO trade,
            Consumer<NormalizedTradeDTO> processor,
            String consumerGroup,
            int partition,
            long offset) {
        
        try {
            // 재시도 가능한 작업 실행
            // 첫 시도
            try {
                processor.accept(trade);
                return true;
            } catch (Exception e) {
                ErrorType errorType = errorClassifier.classify(e);
                
                // 재시도 불가능한 에러는 즉시 DLQ로 전송
                if (!errorClassifier.isRetryable(errorType)) {
                    throw new NonRetryableException(e, errorType);
                }
                
                // 재시도 가능한 에러는 재시도
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
            // 재시도 불가능한 에러 - 즉시 DLQ로 전송
            log.error("Non-retryable error: symbol={}, partition={}, offset={}, errorType={}",
                trade.getSymbol(), partition, offset, e.getErrorType());
            
            dlqService.sendToDLQ(
                topicName,
                partition,
                offset,
                trade,
                e.getCause(),
                consumerGroup,
                0
            );
            
            return false;
            
        } catch (Exception e) {
            // 재시도 실패 또는 예상치 못한 오류
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed after retries or unexpected error: symbol={}, partition={}, offset={}, errorType={}",
                trade.getSymbol(), partition, offset, errorType);
            
            dlqService.sendToDLQ(
                topicName,
                partition,
                offset,
                trade,
                e,
                consumerGroup,
                3 // 최대 재시도 횟수
            );
            
            return false;
        }
    }

    /**
     * 배치 메시지 처리 (재시도 포함)
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
        
        try {
            // 재시도 가능한 작업 실행
            // 첫 시도
            try {
                processor.accept(trades);
                return true;
            } catch (Exception e) {
                ErrorType errorType = errorClassifier.classify(e);
                
                // 재시도 불가능한 에러는 즉시 DLQ로 전송
                if (!errorClassifier.isRetryable(errorType)) {
                    throw new NonRetryableException(e, errorType);
                }
                
                // 재시도 가능한 에러는 재시도
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
            // 재시도 불가능한 에러 - 즉시 DLQ로 전송
            log.error("Non-retryable batch error: size={}, errorType={}",
                trades.size(), e.getErrorType());
            
            dlqService.sendBatchToDLQ(
                topicName,
                trades,
                e.getCause(),
                consumerGroup
            );
            
            return false;
            
        } catch (Exception e) {
            // 재시도 실패 또는 예상치 못한 오류
            ErrorType errorType = errorClassifier.classify(e);
            log.error("Failed batch after retries or unexpected error: size={}, errorType={}",
                trades.size(), errorType);
            
            dlqService.sendBatchToDLQ(
                topicName,
                trades,
                e,
                consumerGroup
            );
            
            return false;
        }
    }

    /**
     * 재시도 불가능한 예외
     */
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
