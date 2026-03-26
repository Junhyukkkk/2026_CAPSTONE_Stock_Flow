package com.stockflow.realtime.consumer;

import com.stockflow.core.util.BatchProcessor;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.retry.RetryableProcessor;
import com.stockflow.realtime.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * StorageConsumer 통합 테스트
 */
@ExtendWith(MockitoExtension.class)
class StorageConsumerTest {

    @Mock
    private RetryableProcessor retryableProcessor;

    @Mock
    private BatchProcessor batchProcessor;

    @Mock
    private PerformanceMetrics performanceMetrics;

    @Mock
    private TransactionManager transactionManager;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private StorageConsumer storageConsumer;

    private List<NormalizedTradeDTO> testTrades;

    @BeforeEach
    void setUp() {
        testTrades = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            testTrades.add(NormalizedTradeDTO.builder()
                .symbol("BTCUSDT")
                .price(new BigDecimal("50000.00"))
                .quantity(new BigDecimal("0.1"))
                .timestamp(Instant.now())
                .source("binance")
                .tradeId("test-trade-id-" + i)
                .build());
        }
    }

    @Test
    void testConsumeStorageTrades_Success() {
        // Given
        doAnswer(invocation -> {
            // batchProcessor.processBatch의 Consumer 실행
            java.util.function.Consumer<List<NormalizedTradeDTO>> processor = 
                invocation.getArgument(1);
            processor.accept(testTrades);
            return null;
        }).when(batchProcessor).processBatch(any(), any());

        when(retryableProcessor.processBatchWithRetry(any(), any(), any()))
            .thenReturn(true);

        // When
        storageConsumer.consumeStorageTrades(testTrades, acknowledgment);

        // Then
        verify(batchProcessor, times(1)).processBatch(any(), any());
        verify(retryableProcessor, times(1)).processBatchWithRetry(any(), any(), any());
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(1)).recordSuccess();
        verify(performanceMetrics, times(1)).recordProcessingTime(anyLong());
    }

    @Test
    void testConsumeStorageTrades_Failure() {
        // Given
        doAnswer(invocation -> {
            java.util.function.Consumer<List<NormalizedTradeDTO>> processor = 
                invocation.getArgument(1);
            processor.accept(testTrades);
            return null;
        }).when(batchProcessor).processBatch(any(), any());

        when(retryableProcessor.processBatchWithRetry(any(), any(), any()))
            .thenReturn(false);

        // When
        storageConsumer.consumeStorageTrades(testTrades, acknowledgment);

        // Then
        verify(batchProcessor, times(1)).processBatch(any(), any());
        verify(retryableProcessor, times(1)).processBatchWithRetry(any(), any(), any());
        verify(acknowledgment, never()).acknowledge();
        verify(performanceMetrics, times(1)).recordFailure();
    }
}
