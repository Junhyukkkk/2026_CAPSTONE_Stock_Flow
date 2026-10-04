package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.config.OptimizationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.test.util.ReflectionTestUtils;
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

    @Mock
    private OptimizationProperties opt;

    @InjectMocks
    private IdempotencyService idempotencyService;

    private NormalizedTradeDTO testTrade;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        // 기본값(false) 유지 — areAlreadyProcessed/markBatchAsProcessed는 파이프라인이 아닌
        // 메시지당 왕복 경로(기존 동작)를 탄다. 일부 테스트는 이 getter를 호출하지 않으므로 lenient.
        lenient().when(opt.isStorageIdempotencyPipeline()).thenReturn(false);

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
                eq(600L),
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
                eq(600L),
                eq(TimeUnit.SECONDS)
        );
    }

    @Test
    void ttlDefaultsTo600() {
        assertEquals(600L, ReflectionTestUtils.getField(idempotencyService, "ttlSeconds"));
    }

    @Test
    void validateTtl_acceptsPositiveValues() {
        assertDoesNotThrow(() -> idempotencyService.validateTtl());

        ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", 86400L);
        assertDoesNotThrow(() -> idempotencyService.validateTtl());
    }

    @Test
    void validateTtl_rejectsZeroAndNegative() {
        for (long bad : new long[]{0L, -1L}) {
            ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", bad);

            IllegalArgumentException e =
                    assertThrows(IllegalArgumentException.class, () -> idempotencyService.validateTtl());
            assertTrue(e.getMessage().contains("STOCKFLOW_IDEMPOTENCY_TTL_SECONDS"));
            assertTrue(e.getMessage().contains(String.valueOf(bad)));
        }
    }

    @Test
    void ttlOverride_appliesToSingleMark() {
        ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", 3600L);

        idempotencyService.markAsProcessed(CH, testTrade);

        verify(valueOperations, times(1)).set(anyString(), eq("1"), eq(3600L), eq(TimeUnit.SECONDS));
    }

    @Test
    void ttlOverride_appliesToBatchMark() {
        ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", 3600L);

        idempotencyService.markBatchAsProcessed(CH, List.of(testTrade, testTrade));

        verify(valueOperations, times(2)).set(anyString(), eq("1"), eq(3600L), eq(TimeUnit.SECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    void ttlOverride_appliesToPipelinedBatchMark() {
        when(opt.isStorageIdempotencyPipeline()).thenReturn(true);
        ReflectionTestUtils.setField(idempotencyService, "ttlSeconds", 3600L);

        idempotencyService.markBatchAsProcessed(CH, List.of(testTrade));

        ArgumentCaptor<RedisCallback<Object>> captor = ArgumentCaptor.forClass(RedisCallback.class);
        verify(redisTemplate).executePipelined(captor.capture());

        RedisConnection connection = mock(RedisConnection.class);
        RedisStringCommands stringCommands = mock(RedisStringCommands.class);
        when(connection.stringCommands()).thenReturn(stringCommands);
        captor.getValue().doInRedis(connection);

        ArgumentCaptor<Expiration> expiration = ArgumentCaptor.forClass(Expiration.class);
        verify(stringCommands).set(any(byte[].class), any(byte[].class), expiration.capture(), any());
        assertEquals(3600L, expiration.getValue().getExpirationTimeInSeconds());
    }
}
