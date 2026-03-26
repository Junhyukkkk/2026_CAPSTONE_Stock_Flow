package com.stockflow.core.retry;

import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * RetryService 테스트
 */
@ExtendWith(MockitoExtension.class)
class RetryServiceTest {

    @Mock
    private ErrorClassifier errorClassifier;

    @InjectMocks
    private RetryService retryService;

    @Test
    void testExecuteWithRetry_SuccessOnFirstAttempt() throws Exception {
        // Given
        AtomicInteger attemptCount = new AtomicInteger(0);
        java.util.function.Supplier<String> operation = () -> {
            attemptCount.incrementAndGet();
            return "success";
        };

        when(errorClassifier.isRetryable(any(ErrorType.class))).thenReturn(true);

        // When
        String result = retryService.executeWithRetry(
            operation,
            ErrorType.PROCESSING_ERROR,
            null
        );

        // Then
        assertEquals("success", result);
        assertEquals(1, attemptCount.get());
    }

    @Test
    void testExecuteWithRetry_SuccessAfterRetries() throws Exception {
        // Given
        AtomicInteger attemptCount = new AtomicInteger(0);
        java.util.function.Supplier<String> operation = () -> {
            int count = attemptCount.incrementAndGet();
            if (count < 3) {
                throw new RuntimeException("Temporary error");
            }
            return "success";
        };

        when(errorClassifier.isRetryable(any(ErrorType.class))).thenReturn(true);

        RetryPolicy policy = RetryPolicy.builder()
            .maxRetries(3)
            .initialDelayMs(10) // 테스트를 위해 짧은 지연
            .maxDelayMs(100)
            .multiplier(2.0)
            .useJitter(false)
            .build();

        // When
        String result = retryService.executeWithRetry(
            operation,
            ErrorType.PROCESSING_ERROR,
            policy
        );

        // Then
        assertEquals("success", result);
        assertEquals(3, attemptCount.get());
    }

    @Test
    void testExecuteWithRetry_MaxRetriesExceeded() {
        // Given
        AtomicInteger attemptCount = new AtomicInteger(0);
        java.util.function.Supplier<String> operation = () -> {
            attemptCount.incrementAndGet();
            throw new RuntimeException("Persistent error");
        };

        when(errorClassifier.isRetryable(any(ErrorType.class))).thenReturn(true);

        RetryPolicy policy = RetryPolicy.builder()
            .maxRetries(2)
            .initialDelayMs(10)
            .maxDelayMs(100)
            .multiplier(2.0)
            .useJitter(false)
            .build();

        // When & Then
        assertThrows(RuntimeException.class, () -> {
            retryService.executeWithRetry(
                operation,
                ErrorType.PROCESSING_ERROR,
                policy
            );
        });

        // 최대 재시도 횟수 + 1 (초기 시도)
        assertEquals(3, attemptCount.get());
    }

    @Test
    void testExecuteWithRetry_NonRetryableError() throws Exception {
        // Given
        AtomicInteger attemptCount = new AtomicInteger(0);
        java.util.function.Supplier<String> operation = () -> {
            attemptCount.incrementAndGet();
            throw new IllegalArgumentException("Validation error");
        };

        when(errorClassifier.isRetryable(ErrorType.VALIDATION_ERROR)).thenReturn(false);

        // When & Then
        assertThrows(IllegalArgumentException.class, () -> {
            retryService.executeWithRetry(
                operation,
                ErrorType.VALIDATION_ERROR,
                null
            );
        });

        // 재시도 없이 1번만 시도
        assertEquals(1, attemptCount.get());
    }
}
