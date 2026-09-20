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

    /**
     * processWithRetry()가 true를 반환하는 경우는 "메시지가 정상 처리됐다"만이 아니라
     * "처리는 실패했지만 DLQ로 durable하게 전송됐다"도 포함한다
     * (RetryableProcessorDefault/RetryableProcessor 계약, Critical #1 수정 참고).
     * 두 경우 모두 오프셋은 커밋되어야 한다 — 그렇지 않으면 컨슈머가 그 오프셋에
     * 영원히 멈추고, retention으로 세그먼트가 삭제되면 이후 메시지가 유실된다.
     */
    @Test
    void testConsumeRealtimeTrade_ProcessingFailsButDlqSendSucceeds_Acknowledges() {
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
    }

    /**
     * processWithRetry()가 false를 반환하는 것은 이제 "DLQ 전송 자체가 실패했다"는 뜻이다.
     * 이 경우에만 오프셋 커밋을 보류해 재처리 기회를 남긴다.
     */
    @Test
    void testConsumeRealtimeTrade_DlqSendFails_DoesNotAcknowledge() {
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
