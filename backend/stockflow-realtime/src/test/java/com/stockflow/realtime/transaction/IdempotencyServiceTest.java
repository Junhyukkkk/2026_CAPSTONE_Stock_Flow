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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * IdempotencyService 테스트
 */
@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

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
    void testIsAlreadyProcessed_True() {
        // Given
        when(redisTemplate.hasKey(anyString())).thenReturn(true);

        // When
        boolean result = idempotencyService.isAlreadyProcessed(testTrade);

        // Then
        assertTrue(result);
        verify(redisTemplate, times(1)).hasKey(anyString());
    }

    @Test
    void testIsAlreadyProcessed_False() {
        // Given
        when(redisTemplate.hasKey(anyString())).thenReturn(false);

        // When
        boolean result = idempotencyService.isAlreadyProcessed(testTrade);

        // Then
        assertFalse(result);
        verify(redisTemplate, times(1)).hasKey(anyString());
    }

    @Test
    void testMarkAsProcessed() {
        // Given
        long ttl = 86400L;

        // When
        idempotencyService.markAsProcessed(testTrade, ttl);

        // Then
        verify(valueOperations, times(1)).set(
            anyString(),
            eq("1"),
            eq(ttl),
            eq(TimeUnit.SECONDS)
        );
    }

    @Test
    void testMarkAsProcessed_DefaultTTL() {
        // When
        idempotencyService.markAsProcessed(testTrade);

        // Then
        verify(valueOperations, times(1)).set(
            anyString(),
            eq("1"),
            eq(86400L),
            eq(TimeUnit.SECONDS)
        );
    }

    @Test
    void testMarkBatchAsProcessed() {
        // Given
        List<NormalizedTradeDTO> trades = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            trades.add(testTrade);
        }

        // When
        idempotencyService.markBatchAsProcessed(trades);

        // Then
        verify(valueOperations, times(5)).set(
            anyString(),
            eq("1"),
            eq(86400L),
            eq(TimeUnit.SECONDS)
        );
    }
}
