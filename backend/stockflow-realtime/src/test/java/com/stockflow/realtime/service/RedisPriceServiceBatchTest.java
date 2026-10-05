package com.stockflow.realtime.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.realtime.config.OptimizationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisPriceServiceBatchTest {

    private RedisTemplate<String, String> redisTemplate;
    private RedisConnection connection;
    private RedisStringCommands stringCommands;
    private RedisPriceService service;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setUp() {
        redisTemplate = mock(RedisTemplate.class);
        ValueOperations<String, String> valueOps = mock(ValueOperations.class);
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get("price:prev-close:BTC")).thenReturn("100");
        connection = mock(RedisConnection.class);
        stringCommands = mock(RedisStringCommands.class);
        when(connection.stringCommands()).thenReturn(stringCommands);
        service = new RedisPriceService(
            redisTemplate, objectMapper, mock(PipelineStageMetrics.class), new OptimizationProperties());
    }

    private static NormalizedTradeDTO trade(String symbol, String price, Long ts) {
        return NormalizedTradeDTO.builder()
            .symbol(symbol).price(new BigDecimal(price)).timestamp(ts).marketType("CRYPTO").build();
    }

    /** executePipelined 에 넘긴 콜백을 mock 커넥션에 대고 실행해 실제로 어떤 명령이 나가는지 관찰한다. */
    private void runCapturedPipeline() {
        ArgumentCaptor<RedisCallback<Object>> cb = ArgumentCaptor.forClass(RedisCallback.class);
        verify(redisTemplate, times(1)).executePipelined(cb.capture());
        cb.getValue().doInRedis(connection);
    }

    private JsonNode json(byte[] bytes) throws Exception {
        return objectMapper.readTree(new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    void sameSymbol_writesOnlyLatestByTimestamp_onePipeline() throws Exception {
        service.processRealtimeTradeBatch(List.of(
            trade("BTC", "110", 1000L),
            trade("BTC", "130", 3000L),   // 최신
            trade("BTC", "120", 2000L)));

        runCapturedPipeline();

        ArgumentCaptor<byte[]> setKey = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<byte[]> setValue = ArgumentCaptor.forClass(byte[].class);
        verify(stringCommands, times(1)).set(setKey.capture(), setValue.capture(), any(), any());
        ArgumentCaptor<byte[]> channel = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<byte[]> message = ArgumentCaptor.forClass(byte[].class);
        verify(connection, times(1)).publish(channel.capture(), message.capture());

        assertThat(new String(setKey.getValue(), StandardCharsets.UTF_8)).isEqualTo("price:latest:BTC");
        assertThat(new String(channel.getValue(), StandardCharsets.UTF_8)).isEqualTo("price:BTC");
        JsonNode snapshot = json(setValue.getValue());
        assertThat(snapshot.get("price").decimalValue()).isEqualByComparingTo("130");
        assertThat(snapshot.get("changePercent").decimalValue()).isEqualByComparingTo("30.00");
        assertThat(message.getValue()).isEqualTo(setValue.getValue());
    }

    @Test
    void sameSymbolSameTimestamp_lastInBatchWins() throws Exception {
        service.processRealtimeTradeBatch(List.of(trade("BTC", "110", 1000L), trade("BTC", "115", 1000L)));

        runCapturedPipeline();

        ArgumentCaptor<byte[]> setValue = ArgumentCaptor.forClass(byte[].class);
        verify(stringCommands).set(any(), setValue.capture(), any(), any());
        assertThat(json(setValue.getValue()).get("price").decimalValue()).isEqualByComparingTo("115");
    }

    @Test
    void differentSymbols_eachWrittenOnce() {
        service.processRealtimeTradeBatch(List.of(
            trade("BTC", "110", 1000L), trade("ETH", "50", 1000L), trade("BTC", "111", 2000L)));

        runCapturedPipeline();

        ArgumentCaptor<byte[]> keys = ArgumentCaptor.forClass(byte[].class);
        verify(stringCommands, times(2)).set(keys.capture(), any(), any(), any());
        List<String> written = new ArrayList<>();
        keys.getAllValues().forEach(k -> written.add(new String(k, StandardCharsets.UTF_8)));
        assertThat(written).containsExactlyInAnyOrder("price:latest:BTC", "price:latest:ETH");
        verify(connection, times(2)).publish(any(), any());
    }

    @Test
    void invalidPriceAnywhereInBatch_throwsBeforeAnyWrite() {
        List<NormalizedTradeDTO> trades = List.of(trade("BTC", "110", 2000L), trade("BTC", "0", 1000L));

        assertThatThrownBy(() -> service.processRealtimeTradeBatch(trades))
            .isInstanceOf(IllegalArgumentException.class);
        verify(redisTemplate, never()).executePipelined(any(RedisCallback.class));
    }

    @Test
    void emptyBatch_doesNothing() {
        service.processRealtimeTradeBatch(List.of());

        verify(redisTemplate, never()).executePipelined(any(RedisCallback.class));
        verify(redisTemplate, never()).opsForValue();
        verify(redisTemplate, never()).convertAndSend(anyString(), any());
    }
}
