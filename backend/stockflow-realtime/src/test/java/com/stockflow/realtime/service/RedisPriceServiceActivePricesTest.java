package com.stockflow.realtime.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.realtime.config.OptimizationProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * RedisPriceService.getActivePrices() 단위 테스트.
 * 실시간 시세 화면이 초기 워치리스트를 자동 구성하는 데 쓰는 경로.
 */
@ExtendWith(MockitoExtension.class)
class RedisPriceServiceActivePricesTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private PipelineStageMetrics stageMetrics;

    private RedisPriceService newService() {
        return new RedisPriceService(redisTemplate, new ObjectMapper(), stageMetrics, new OptimizationProperties());
    }

    private String json(String symbol, String price) {
        return "{\"symbol\":\"" + symbol + "\",\"price\":" + price + ",\"marketType\":\"CRYPTO\"}";
    }

    /** SCAN 커서를 흉내내는 mock — hasNext()/next() 를 주어진 키 순서대로 리턴하고 close()는 no-op. */
    @SuppressWarnings("unchecked")
    private Cursor<String> cursorOf(String... keys) {
        Cursor<String> cursor = mock(Cursor.class);
        java.util.Iterator<String> it = Arrays.asList(keys).iterator();
        // 키가 없으면 next()는 호출되지 않으므로 lenient (strict-stubs에서 미사용 스텁 오류 방지)
        lenient().when(cursor.hasNext()).thenAnswer(inv -> it.hasNext());
        lenient().when(cursor.next()).thenAnswer(inv -> it.next());
        return cursor;
    }

    @Test
    void returnsEmptyList_whenNoKeys() {
        Cursor<String> emptyCursor = cursorOf();
        when(redisTemplate.scan(any(ScanOptions.class))).thenReturn(emptyCursor);

        assertThat(newService().getActivePrices()).isEmpty();
    }

    @Test
    void returnsSnapshotsSortedBySymbol_skippingNullsAndBadJson() {
        Cursor<String> cursor = cursorOf(
                "price:latest:ETHUSDT", "price:latest:BTCUSDT", "price:latest:GONE", "price:latest:BAD");
        when(redisTemplate.scan(any(ScanOptions.class))).thenReturn(cursor);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.multiGet(any())).thenReturn(Arrays.asList(
                json("ETHUSDT", "3000"),
                json("BTCUSDT", "50000"),
                null,              // 키는 있었으나 만료됨
                "not-a-json"       // 깨진 값
        ));

        List<PriceSnapshot> result = newService().getActivePrices();

        assertThat(result).extracting(PriceSnapshot::getSymbol).containsExactly("BTCUSDT", "ETHUSDT");
        assertThat(result.get(0).getPrice()).isEqualByComparingTo(new BigDecimal("50000"));
    }
}
