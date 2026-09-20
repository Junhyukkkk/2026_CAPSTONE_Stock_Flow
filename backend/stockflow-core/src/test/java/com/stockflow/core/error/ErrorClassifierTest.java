package com.stockflow.core.error;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ErrorClassifier 테스트
 *
 * isRetryable()이 ErrorType의 문서화된 처리 방침(javadoc)과 일치하는지 검증한다.
 */
class ErrorClassifierTest {

    private final ErrorClassifier classifier = new ErrorClassifier();

    @Test
    void validationError_isNotRetryable() {
        // ErrorType.VALIDATION_ERROR 의 javadoc: "처리: DLQ 전송 (재시도 불필요)"
        // 잘못된 형식/누락 필드는 재시도로 해결되지 않으므로 재시도 대상이 아니어야 한다.
        assertFalse(classifier.isRetryable(ErrorType.VALIDATION_ERROR));
    }

    @Test
    void storageConnectionError_isRetryable() {
        assertTrue(classifier.isRetryable(ErrorType.STORAGE_CONNECTION_ERROR));
    }

    @Test
    void storageError_isRetryable() {
        assertTrue(classifier.isRetryable(ErrorType.STORAGE_ERROR));
    }

    @Test
    void timeoutError_isRetryable() {
        assertTrue(classifier.isRetryable(ErrorType.TIMEOUT_ERROR));
    }

    @Test
    void processingError_isNotRetryable() {
        assertFalse(classifier.isRetryable(ErrorType.PROCESSING_ERROR));
    }

    @Test
    void unknownError_isNotRetryable() {
        assertFalse(classifier.isRetryable(ErrorType.UNKNOWN_ERROR));
    }

    @ParameterizedTest
    @EnumSource(value = ErrorType.class, names = {"STORAGE_CONNECTION_ERROR", "STORAGE_ERROR", "TIMEOUT_ERROR"})
    void onlyTransientStorageAndTimeoutErrorsAreRetryable(ErrorType errorType) {
        assertTrue(classifier.isRetryable(errorType));
    }

    @ParameterizedTest
    @EnumSource(value = ErrorType.class, names = {"VALIDATION_ERROR", "PROCESSING_ERROR", "UNKNOWN_ERROR"})
    void nonTransientErrorsAreNeverRetryable(ErrorType errorType) {
        assertFalse(classifier.isRetryable(errorType));
    }
}
