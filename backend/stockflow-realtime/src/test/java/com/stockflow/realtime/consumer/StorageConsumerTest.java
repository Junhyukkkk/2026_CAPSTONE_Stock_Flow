package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PerformanceMetrics;
import com.stockflow.realtime.storage.StorageService;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * StorageConsumer 단위 테스트
 */
@ExtendWith(MockitoExtension.class)
class StorageConsumerTest {

    @Mock
    private StorageService storageService;

    @Mock
    private PerformanceMetrics performanceMetrics;

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
        when(storageService.saveBatch(any(), any())).thenReturn(true);

        storageConsumer.consumeStorageTrades(testTrades, acknowledgment);

        verify(storageService, times(1)).saveBatch(any(), any());
        verify(acknowledgment, times(1)).acknowledge();
        verify(performanceMetrics, times(1)).recordSuccess();
        verify(performanceMetrics, times(1)).recordProcessingTime(anyLong());
    }

    @Test
    void testConsumeStorageTrades_Failure() {
        when(storageService.saveBatch(any(), any())).thenReturn(false);

        assertThrows(RuntimeException.class, () -> {
            storageConsumer.consumeStorageTrades(testTrades, acknowledgment);
        });

        verify(storageService, times(1)).saveBatch(any(), any());
        verify(acknowledgment, never()).acknowledge();
        verify(performanceMetrics, times(1)).recordFailure();
    }

    @Test
    void testConsumeStorageTrades_EmptyList() {
        storageConsumer.consumeStorageTrades(new ArrayList<>(), acknowledgment);

        verify(storageService, never()).saveBatch(any(), any());
        verify(acknowledgment, times(1)).acknowledge();
    }
}
