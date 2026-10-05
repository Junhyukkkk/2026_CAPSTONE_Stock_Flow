package com.stockflow.realtime.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.realtime.config.OptimizationProperties;
import com.stockflow.realtime.redis.PriceKeys;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisPriceServiceTest {

    @Mock RedisTemplate<String, String> redisTemplate;
    @Mock ValueOperations<String, String> valueOps;

    OptimizationProperties opt = new OptimizationProperties();
    RedisPriceService service;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        service = newService(new ObjectMapper());
    }

    private RedisPriceService newService(ObjectMapper mapper) {
        return new RedisPriceService(redisTemplate, mapper,
                new PipelineStageMetrics(new SimpleMeterRegistry()), opt);
    }

    private static NormalizedTradeDTO trade(String price) {
        return NormalizedTradeDTO.builder().source("BINANCE").symbol("BTCUSDT")
                .price(price == null ? null : new BigDecimal(price)).volume(BigDecimal.ONE).tradeId("1")
                .exchange("BINANCE").timestamp(1L).receivedAt(2L).marketType("CRYPTO").build();
    }

    @Test
    void rejectsMissingOrNonPositivePrice() {
        assertThatThrownBy(() -> service.processRealtimeTrade(trade(null))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.processRealtimeTrade(trade("0"))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.processRealtimeTrade(trade("-5"))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void pipelinedPathWritesSetAndPublishInOneRoundTrip() {
        opt.setRedisPipeline(true);
        when(valueOps.get(PriceKeys.prevClose("BTCUSDT"))).thenReturn("100");

        service.processRealtimeTrade(trade("110"));

        ArgumentCaptor<RedisCallback<Object>> callback = ArgumentCaptor.forClass(RedisCallback.class);
        verify(redisTemplate).executePipelined(callback.capture());
        RedisConnection connection = mock(RedisConnection.class);
        RedisStringCommands strings = mock(RedisStringCommands.class);
        when(connection.stringCommands()).thenReturn(strings);
        callback.getValue().doInRedis(connection);
        verify(strings).set(any(byte[].class), any(byte[].class), any(), any());
        verify(connection).publish(any(byte[].class), any(byte[].class));
        verify(valueOps, never()).set(anyString(), anyString(), any(java.time.Duration.class));
    }

    @Test
    void sequentialPathSetsThenPublishesWithChangeAgainstPrevClose() throws Exception {
        opt.setRedisPipeline(false);
        when(valueOps.get(PriceKeys.prevClose("BTCUSDT"))).thenReturn("100");

        service.processRealtimeTrade(trade("110"));

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(PriceKeys.latestPrice("BTCUSDT")), json.capture(), eq(PriceKeys.LATEST_PRICE_TTL));
        PriceSnapshot snapshot = new ObjectMapper().readValue(json.getValue(), PriceSnapshot.class);
        assertThat(snapshot.getChange()).isEqualByComparingTo("10");
        assertThat(snapshot.getChangePercent()).isEqualByComparingTo("10.00");
        verify(redisTemplate).convertAndSend(eq(PriceKeys.priceChannel("BTCUSDT")), anyString());
    }

    @Test
    void changeIsZeroWithoutPositivePrevClose() throws Exception {
        opt.setRedisPipeline(false);
        when(valueOps.get(anyString())).thenReturn(null, "0");

        service.processRealtimeTrade(trade("5"));
        service.processRealtimeTrade(trade("5"));

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(valueOps, times(2)).set(anyString(), json.capture(), any(java.time.Duration.class));
        for (String value : json.getAllValues()) {
            assertThat(new ObjectMapper().readValue(value, PriceSnapshot.class).getChange()).isEqualByComparingTo("0");
        }
    }

    @Test
    void serializationFailureOnSavePropagatesButPublishFailureIsSwallowed() throws Exception {
        ObjectMapper failing = mock(ObjectMapper.class);
        when(failing.writeValueAsString(any())).thenThrow(new JsonProcessingException("boom") {
        });
        RedisPriceService failingService = newService(failing);

        opt.setRedisPipeline(true);
        assertThatThrownBy(() -> failingService.processRealtimeTrade(trade("5"))).hasMessageContaining("cache latest price");
        opt.setRedisPipeline(false);
        assertThatThrownBy(() -> failingService.processRealtimeTrade(trade("5"))).hasMessageContaining("cache latest price");
    }

    @Test
    void getLatestPriceHandlesMissingAndCorruptValues() {
        when(valueOps.get(PriceKeys.latestPrice("A"))).thenReturn(null);
        when(valueOps.get(PriceKeys.latestPrice("B"))).thenReturn("{not json");
        when(valueOps.get(PriceKeys.latestPrice("C"))).thenReturn("{\"symbol\":\"C\",\"price\":1}");

        assertThat(service.getLatestPrice("A")).isNull();
        assertThat(service.getLatestPrice("B")).isNull();
        assertThat(service.getLatestPrice("C").getSymbol()).isEqualTo("C");
    }

    @Test
    void previousCloseIsCachedLocallyUntilInvalidated() {
        opt.setPrevCloseLocalCache(true);
        when(valueOps.get(PriceKeys.prevClose("ETH"))).thenReturn("50");

        assertThat(service.getPreviousClose("ETH")).isEqualByComparingTo("50");
        assertThat(service.getPreviousClose("ETH")).isEqualByComparingTo("50");
        verify(valueOps, times(1)).get(PriceKeys.prevClose("ETH"));

        service.setPreviousClose("ETH", new BigDecimal("60"));
        verify(valueOps).set(PriceKeys.prevClose("ETH"), "60", PriceKeys.PREV_CLOSE_TTL);
        when(valueOps.get(PriceKeys.prevClose("ETH"))).thenReturn("60");
        assertThat(service.getPreviousClose("ETH")).isEqualByComparingTo("60");
        verify(valueOps, times(2)).get(PriceKeys.prevClose("ETH"));
    }

    @Test
    void previousCloseWithoutLocalCacheAlwaysHitsRedis() {
        opt.setPrevCloseLocalCache(false);
        when(valueOps.get(PriceKeys.prevClose("ETH"))).thenReturn(null);

        assertThat(service.getPreviousClose("ETH")).isNull();
        assertThat(service.getPreviousClose("ETH")).isNull();
        verify(valueOps, times(2)).get(PriceKeys.prevClose("ETH"));
    }

    @Test
    void expiredLocalCacheEntryIsRefetched() {
        opt.setPrevCloseLocalCache(true);
        opt.setPrevCloseLocalCacheTtlMs(0);
        when(valueOps.get(PriceKeys.prevClose("ETH"))).thenReturn("1");

        service.getPreviousClose("ETH");
        service.getPreviousClose("ETH");

        verify(valueOps, times(2)).get(PriceKeys.prevClose("ETH"));
    }

    @Test
    void loadPreviousClosesSkipsNullValues() {
        Map<String, BigDecimal> closes = new HashMap<>();
        closes.put("A", new BigDecimal("1.50"));
        closes.put("B", null);

        assertThat(service.loadPreviousCloses(closes)).isEqualTo(2);

        verify(valueOps).set(PriceKeys.prevClose("A"), "1.50", PriceKeys.PREV_CLOSE_TTL);
        verify(valueOps, never()).set(eq(PriceKeys.prevClose("B")), anyString(), any(java.time.Duration.class));
    }
}
