package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessorInterface;
import com.stockflow.realtime.service.RedisPriceService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * RealtimeConsumer 단위 테스트
 */
@ExtendWith(MockitoExtension.class)
class RealtimeConsumerTest {

    @Mock
    private RetryableProcessorInterface retryableProcessor;

    @Mock
    private PerformanceMetrics performanceMetrics;

    @Mock
    private RedisPriceService redisPriceService;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private RealtimeConsumer realtimeConsumer;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
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

    @Test
    void testConsumeRealtimeTrade_Success() {
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong()))
                .thenReturn(true);

        realtimeConsumer.consumeRealtimeTrade(testTrade, acknowledgment, 0, 100L);

        verify(retryableProcessor, times(1)).processWithRetry(
                eq(testTrade),
                any(),
                any(),
                eq(0),
                eq(100L)
        );
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(1)).recordSuccessWithLatency(testTrade.getTimestamp());
        verify(performanceMetrics, times(1)).recordProcessingTime(anyLong());
    }

    @Test
    void testConsumeRealtimeTrade_Failure() {
        when(retryableProcessor.processWithRetry(any(), any(), any(), anyInt(), anyLong()))
                .thenReturn(false);

        realtimeConsumer.consumeRealtimeTrade(testTrade, acknowledgment, 0, 100L);

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
