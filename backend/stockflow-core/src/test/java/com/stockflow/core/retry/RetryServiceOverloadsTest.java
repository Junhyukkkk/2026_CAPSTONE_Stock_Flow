package com.stockflow.core.retry;

import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** executeWithRetry 의 오버로드·백오프 경로. 지연은 1ms 로 줄여 사용한다. */
class RetryServiceOverloadsTest {

    private final RetryService service = new RetryService(new ErrorClassifier());
    private final RetryPolicy fast = RetryPolicy.builder()
            .maxRetries(2).initialDelayMs(1).maxDelayMs(2).multiplier(2.0).useJitter(true).jitterRatio(0.5).build();

    @Test
    void retriesUntilSuccessWithErrorTypeAndPolicy() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        String result = service.executeWithRetry(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("flaky");
            }
            return "done";
        }, ErrorType.STORAGE_ERROR, fast);

        assertThat(result).isEqualTo("done");
        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void givesUpAfterMaxRetriesAndRethrowsLastException() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> service.executeWithRetry(() -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always");
        }, ErrorType.TIMEOUT_ERROR, fast)).hasMessage("always");

        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void nonRetryableErrorRunsOnce() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> service.executeWithRetry(() -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("bad input");
        }, ErrorType.VALIDATION_ERROR, fast)).isInstanceOf(IllegalArgumentException.class);

        assertThat(calls.get()).isEqualTo(1);
    }

    @Test
    void nullPolicyFallsBackToDefaultForNonRetryableFastPath() throws Exception {
        assertThat(service.executeWithRetry(() -> "x", ErrorType.PROCESSING_ERROR, null)).isEqualTo("x");
        assertThat(service.executeWithRetry(() -> "y", ErrorType.UNKNOWN_ERROR)).isEqualTo("y");
    }

    @Test
    void throwableOverloadClassifiesFirst() throws Exception {
        // ConnectException → STORAGE_CONNECTION_ERROR(재시도 대상)이지만 첫 시도에 성공하면 즉시 반환
        assertThat(service.executeWithRetry(() -> "ok", new ConnectException("down"))).isEqualTo("ok");
        // IllegalArgumentException → VALIDATION_ERROR(재시도 안 함)
        assertThat(service.executeWithRetry(() -> "ok", new IllegalArgumentException("x"))).isEqualTo("ok");
    }
}
