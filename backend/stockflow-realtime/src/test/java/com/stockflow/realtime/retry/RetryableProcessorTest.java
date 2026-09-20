package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.realtime.dlq.DLQService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * RetryableProcessor (retry.mode=async) 단위 테스트.
 *
 * 이 구현은 이미 "DLQ/재시도 토픽 전송 후 커밋"을 따르고 있었다 (Critical #1 은
 * RetryableProcessorDefault 에만 있던 버그). 여기서는 그 기존 계약을 회귀 테스트로 고정한다.
 */
@ExtendWith(MockitoExtension.class)
class RetryableProcessorTest {

    @Mock
    private ErrorClassifier errorClassifier;

    @Mock
    private DLQService dlqService;

    @Mock
    private RetryTopicService retryTopicService;

    private RetryableProcessor retryableProcessor;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        retryableProcessor = new RetryableProcessor(errorClassifier, dlqService, retryTopicService);
        ReflectionTestUtils.setField(retryableProcessor, "topicName", "market.normalized");
        ReflectionTestUtils.setField(retryableProcessor, "maxRetries", 3);

        long now = System.currentTimeMillis();
        testTrade = NormalizedTradeDTO.builder()
                .symbol("BTCUSDT")
                .price(new BigDecimal("50000.00"))
                .volume(new BigDecimal("0.1"))
                .timestamp(now)
                .receivedAt(now)
                .source("BINANCE")
                .tradeId("test-trade-id-1")
                .exchange("BINANCE")
                .marketType("CRYPTO")
                .build();
    }

    @SuppressWarnings("unchecked")
    private Consumer<NormalizedTradeDTO> mockProcessor() {
        return mock(Consumer.class);
    }

    @Test
    void processWithRetry_success_doesNotTouchDlqOrRetryTopic() {
        Consumer<NormalizedTradeDTO> processor = mockProcessor();

        boolean result = retryableProcessor.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertTrue(result);
        verifyNoInteractions(dlqService, retryTopicService);
    }

    @Test
    void processWithRetry_nonRetryableError_sendsToDlq_returnsTrue() {
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw new IllegalArgumentException("bad payload");
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.VALIDATION_ERROR);
        when(errorClassifier.isRetryable(ErrorType.VALIDATION_ERROR)).thenReturn(false);

        boolean result = retryableProcessor.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertTrue(result);
        verify(dlqService, times(1)).sendToDLQ(
                eq("market.normalized"), eq(0), eq(100L), eq(testTrade), any(), eq("realtime-group"), eq(0));
        verifyNoInteractions(retryTopicService);
    }

    @Test
    void processWithRetry_maxRetriesExceeded_sendsToDlq_returnsTrue() {
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw new RuntimeException("storage down");
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.STORAGE_ERROR);
        when(errorClassifier.isRetryable(ErrorType.STORAGE_ERROR)).thenReturn(true);

        boolean result = retryableProcessor.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L, 3);

        assertTrue(result);
        verify(dlqService, times(1)).sendToDLQ(
                eq("market.normalized"), eq(0), eq(100L), eq(testTrade), any(), eq("realtime-group"), eq(3));
    }

    @Test
    void processWithRetry_retryableWithinLimit_sendsToRetryTopic_returnsTrue() {
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw new RuntimeException("transient error");
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.STORAGE_ERROR);
        when(errorClassifier.isRetryable(ErrorType.STORAGE_ERROR)).thenReturn(true);

        boolean result = retryableProcessor.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L, 0);

        assertTrue(result);
        verify(retryTopicService, times(1)).sendToRetryTopic(eq(testTrade), eq(1), any());
        verifyNoInteractions(dlqService);
    }
}
