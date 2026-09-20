package com.stockflow.realtime.retry;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.error.ErrorClassifier;
import com.stockflow.core.error.ErrorType;
import com.stockflow.core.retry.RetryPolicy;
import com.stockflow.core.retry.RetryService;
import com.stockflow.realtime.dlq.DLQService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * RetryableProcessorDefault (retry.mode=sync, 운영 기본값) 단위 테스트.
 *
 * Critical #1 수정 검증: DLQ 전송이 성공하면 오프셋을 커밋해야 하고(true 반환),
 * DLQ 전송 자체가 실패한 경우에만 커밋을 보류해야 한다(false 반환).
 */
@ExtendWith(MockitoExtension.class)
class RetryableProcessorDefaultTest {

    @Mock
    private RetryService retryService;

    @Mock
    private ErrorClassifier errorClassifier;

    @Mock
    private DLQService dlqService;

    @Mock
    private RetryPolicy retryPolicy;

    private RetryableProcessorDefault retryableProcessorDefault;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        retryableProcessorDefault =
                new RetryableProcessorDefault(retryService, errorClassifier, dlqService, retryPolicy);
        ReflectionTestUtils.setField(retryableProcessorDefault, "topicName", "market.normalized");

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
    void processWithRetry_success_doesNotTouchDlq() {
        Consumer<NormalizedTradeDTO> processor = mockProcessor();

        boolean result = retryableProcessorDefault.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertTrue(result);
        verifyNoInteractions(dlqService);
    }

    @Test
    void processWithRetry_nonRetryableError_dlqSucceeds_returnsTrue() {
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw new IllegalArgumentException("bad payload");
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.VALIDATION_ERROR);
        when(errorClassifier.isRetryable(ErrorType.VALIDATION_ERROR)).thenReturn(false);

        boolean result = retryableProcessorDefault.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertTrue(result, "offset should be acknowledged once the message is durably routed to the DLQ");
        verify(dlqService, times(1)).sendToDLQ(
                eq("market.normalized"), eq(0), eq(100L), eq(testTrade), any(), eq("realtime-group"), eq(0));
    }

    @Test
    void processWithRetry_retriesExhausted_dlqSucceeds_returnsTrue() throws Exception {
        RuntimeException boom = new RuntimeException("storage down");
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw boom;
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.STORAGE_ERROR);
        when(errorClassifier.isRetryable(ErrorType.STORAGE_ERROR)).thenReturn(true);
        when(retryService.executeWithRetry(any(), eq(ErrorType.STORAGE_ERROR), eq(retryPolicy)))
                .thenThrow(boom);

        boolean result = retryableProcessorDefault.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertTrue(result);
        verify(dlqService, times(1)).sendToDLQ(
                eq("market.normalized"), eq(0), eq(100L), eq(testTrade), eq(boom), eq("realtime-group"), eq(3));
    }

    @Test
    void processWithRetry_dlqSendItselfFails_returnsFalse() {
        Consumer<NormalizedTradeDTO> processor = t -> {
            throw new IllegalArgumentException("bad payload");
        };
        when(errorClassifier.classify(any())).thenReturn(ErrorType.VALIDATION_ERROR);
        when(errorClassifier.isRetryable(ErrorType.VALIDATION_ERROR)).thenReturn(false);
        doThrow(new RuntimeException("kafka producer down"))
                .when(dlqService).sendToDLQ(any(), anyInt(), anyLong(), any(), any(), any(), anyInt());

        boolean result = retryableProcessorDefault.processWithRetry(
                testTrade, processor, "realtime-group", 0, 100L);

        assertFalse(result, "ack must be withheld when the DLQ send itself fails");
    }
}
