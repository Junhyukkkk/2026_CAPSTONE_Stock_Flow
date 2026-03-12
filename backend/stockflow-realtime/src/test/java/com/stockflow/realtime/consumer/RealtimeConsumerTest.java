package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.performance.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessor;
import com.stockflow.realtime.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.GenericMessage;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * RealtimeConsumer 통합 테스트
 */
@ExtendWith(MockitoExtension.class)
class RealtimeConsumerTest {

    @Mock
    private RetryableProcessor retryableProcessor;

    @Mock
    private PerformanceMetrics performanceMetrics;

    @Mock
    private TransactionManager transactionManager;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private RealtimeConsumer realtimeConsumer;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        testTrade = NormalizedTradeDTO.builder()
            .symbol("BTCUSDT")
            .price(new BigDecimal("50000.00"))
            .quantity(new BigDecimal("0.1"))
            .timestamp(Instant.now())
            .source("binance")
            .tradeId("test-trade-id-1")
            .build();
    }

    @Test
    void testConsumeRealtimeTrade_Success() {
        // Given
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong()))
            .thenReturn(true);

        // When
        realtimeConsumer.consumeRealtimeTrade(
            testTrade,
            acknowledgment,
            0,
            100L
        );

        // Then
        verify(retryableProcessor, times(1)).processWithRetry(
            eq(testTrade),
            any(),
            any(),
            eq(0),
            eq(100L)
        );
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(1)).recordSuccess();
        verify(performanceMetrics, times(1)).recordProcessingTime(anyLong());
    }

    @Test
    void testConsumeRealtimeTrade_Failure() {
        // Given
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong()))
            .thenReturn(false);

        // When
        realtimeConsumer.consumeRealtimeTrade(
            testTrade,
            acknowledgment,
            0,
            100L
        );

        // Then
        verify(retryableProcessor, times(1)).processWithRetry(
            eq(testTrade),
            any(),
            any(),
            eq(0),
            eq(100L)
        );
        verify(acknowledgment, never()).acknowledge();
        verify(performanceMetrics, times(1)).recordFailure();
    }
}
