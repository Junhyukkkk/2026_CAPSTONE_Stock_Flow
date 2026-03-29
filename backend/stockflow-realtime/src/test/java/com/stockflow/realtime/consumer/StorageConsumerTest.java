package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.core.util.BatchProcessor;
import com.stockflow.realtime.retry.RetryableProcessorInterface;
import com.stockflow.realtime.storage.MarketTickBulkWriter;
import com.stockflow.realtime.transaction.RealtimeTransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * StorageConsumer 단위 테스트
 */
@ExtendWith(MockitoExtension.class)
class StorageConsumerTest {

    @Mock
    private RetryableProcessorInterface retryableProcessor;

    @Mock
    private BatchProcessor batchProcessor;

    @Mock
    private PerformanceMetrics performanceMetrics;

    @Mock
    private RealtimeTransactionManager transactionManager;

    @Mock
    private MarketTickBulkWriter marketTickBulkWriter;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private StorageConsumer storageConsumer;

    private List<NormalizedTradeDTO> testTrades;

    @BeforeEach
    void setUp() {
        testTrades = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            testTrades.add(NormalizedTradeDTO.builder()
                    .source("BINANCE")
                    .symbol("BTCUSDT")
                    .price(new BigDecimal("50000.00"))
                    .volume(new BigDecimal("0.1"))
                    .tradeId("test-trade-id-" + i)
                    .exchange("BINANCE")
                    .timestamp(now)
                    .receivedAt(now)
                    .marketType("CRYPTO")
                    .build());
        }
    }

    @Test
    void testConsumeStorageTrades_Success() {
        doAnswer(invocation -> {
            var processor = invocation.<java.util.function.Consumer<List<NormalizedTradeDTO>>>getArgument(1);
            processor.accept(testTrades);
            return null;
        }).when(batchProcessor).processBatch(any(), any());

        when(retryableProcessor.processBatchWithRetry(any(), any(), any()))
                .thenReturn(true);

        storageConsumer.consumeStorageTrades(testTrades, acknowledgment);

        verify(batchProcessor, times(1)).processBatch(any(), any());
        verify(retryableProcessor, times(1)).processBatchWithRetry(any(), any(), any());
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(1)).recordSuccess();
        verify(performanceMetrics, times(1)).recordProcessingTime(anyLong());
    }

    @Test
    void testConsumeStorageTrades_Failure() {
        doAnswer(invocation -> {
            var processor = invocation.<java.util.function.Consumer<List<NormalizedTradeDTO>>>getArgument(1);
            processor.accept(testTrades);
            return null;
        }).when(batchProcessor).processBatch(any(), any());

        when(retryableProcessor.processBatchWithRetry(any(), any(), any()))
                .thenReturn(false);

        storageConsumer.consumeStorageTrades(testTrades, acknowledgment);

        verify(batchProcessor, times(1)).processBatch(any(), any());
        verify(retryableProcessor, times(1)).processBatchWithRetry(any(), any(), any());
        verify(acknowledgment, never()).acknowledge();
        verify(performanceMetrics, times(1)).recordFailure();
    }
}
