package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    private static final String CH = IdempotencyChannels.STORAGE;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @InjectMocks
    private IdempotencyService idempotencyService;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

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
    void testIsAlreadyProcessed_True() {
        when(redisTemplate.hasKey(anyString())).thenReturn(true);

        boolean result = idempotencyService.isAlreadyProcessed(CH, testTrade);

        assertTrue(result);
        verify(redisTemplate, times(1)).hasKey(argThat((String key) ->
                key.startsWith("processed:storage:BTCUSDT:BINANCE:test-trade-id-1:")
                        && key.endsWith(String.valueOf(testTrade.getTimestamp()))));
    }

    @Test
    void testIsAlreadyProcessed_False() {
        when(redisTemplate.hasKey(anyString())).thenReturn(false);

        boolean result = idempotencyService.isAlreadyProcessed(CH, testTrade);

        assertFalse(result);
    }

    @Test
    void testMarkAsProcessed() {
        long ttl = 86400L;

        idempotencyService.markAsProcessed(CH, testTrade, ttl);

        verify(valueOperations, times(1)).set(
                argThat((String key) ->
                        key.contains("processed:storage:BTCUSDT:BINANCE:test-trade-id-1")),
                eq("1"),
                eq(ttl),
                eq(TimeUnit.SECONDS)
        );
    }

    @Test
    void testMarkAsProcessed_DefaultTTL() {
        idempotencyService.markAsProcessed(CH, testTrade);

        verify(valueOperations, times(1)).set(
                anyString(),
                eq("1"),
                eq(86400L),
                eq(TimeUnit.SECONDS)
        );
    }

    @Test
    void testMarkBatchAsProcessed() {
        List<NormalizedTradeDTO> trades = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            trades.add(testTrade);
        }

        idempotencyService.markBatchAsProcessed(CH, trades);

        verify(valueOperations, times(5)).set(
                anyString(),
                eq("1"),
                eq(86400L),
                eq(TimeUnit.SECONDS)
        );
    }
}
