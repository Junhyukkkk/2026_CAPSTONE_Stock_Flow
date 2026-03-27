package com.stockflow.core.retry;

import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.function.Supplier;

/**
 * 재시도 서비스
 *
 * Exponential Backoff를 사용한 재시도 로직
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RetryService {

    private final ErrorClassifier errorClassifier;
    private final Random random = new Random();

    /**
     * 기본 재시도 정책
     */
    private static final RetryPolicy DEFAULT_POLICY = RetryPolicy.builder()
        .maxRetries(3)
        .initialDelayMs(1000)
        .maxDelayMs(60000)
        .multiplier(2.0)
        .useJitter(true)
        .jitterRatio(0.2)
        .build();

    /**
     * 재시도 가능한 작업 실행
     *
     * @param operation 실행할 작업
     * @param errorType 에러 타입
     * @param policy 재시도 정책 (null이면 기본 정책 사용)
     * @return 작업 결과
     * @throws Exception 최대 재시도 후에도 실패 시
     */
    public <T> T executeWithRetry(
            Supplier<T> operation,
            ErrorType errorType,
            RetryPolicy policy) throws Exception {

        if (policy == null) {
            policy = DEFAULT_POLICY;
        }

        // 재시도 불가능한 에러는 즉시 실패
        if (!errorClassifier.isRetryable(errorType)) {
            log.debug("Error is not retryable: {}", errorType);
            return operation.get();
        }

        Exception lastException = null;

        for (int attempt = 0; attempt <= policy.getMaxRetries(); attempt++) {
            try {
                if (attempt > 0) {
                    long delay = calculateDelay(attempt, policy);
                    log.debug("Retrying operation: attempt={}/{}, delay={}ms, errorType={}",
                        attempt, policy.getMaxRetries(), delay, errorType);

                    Thread.sleep(delay);
                }

                return operation.get();

            } catch (Exception e) {
                lastException = e;
                log.warn("Operation failed: attempt={}/{}, errorType={}, error={}",
                    attempt, policy.getMaxRetries(), errorType, e.getMessage());

                // 마지막 시도인 경우
                if (attempt >= policy.getMaxRetries()) {
                    log.error("Max retries exceeded: attempts={}, errorType={}",
                        attempt + 1, errorType);
                    break;
                }
            }
        }

        // 모든 재시도 실패
        throw lastException != null ? lastException : new RuntimeException("Operation failed after retries");
    }

    /**
     * Exponential Backoff 지연 시간 계산
     *
     * @param attempt 현재 시도 횟수 (0부터 시작)
     * @param policy 재시도 정책
     * @return 지연 시간 (밀리초)
     */
    private long calculateDelay(int attempt, RetryPolicy policy) {
        // Exponential Backoff: initialDelay * (multiplier ^ (attempt - 1))
        long delay = (long) (policy.getInitialDelayMs() * Math.pow(policy.getMultiplier(), attempt - 1));

        // 최대 지연 시간 제한
        delay = Math.min(delay, policy.getMaxDelayMs());

        // 지터 적용 (Thundering Herd 문제 방지)
        if (policy.isUseJitter()) {
            double jitter = delay * policy.getJitterRatio();
            double randomJitter = (random.nextDouble() * 2 - 1) * jitter; // -jitter ~ +jitter
            delay = (long) (delay + randomJitter);
            delay = Math.max(0, delay); // 음수 방지
        }

        return delay;
    }

    /**
     * 재시도 가능한 작업 실행 (기본 정책 사용)
     */
    public <T> T executeWithRetry(Supplier<T> operation, ErrorType errorType) throws Exception {
        return executeWithRetry(operation, errorType, null);
    }

    /**
     * 재시도 가능한 작업 실행 (에러 타입 자동 판단)
     */
    public <T> T executeWithRetry(Supplier<T> operation, Throwable exception) throws Exception {
        ErrorType errorType = errorClassifier.classify(exception);
        return executeWithRetry(operation, errorType, null);
    }
}
