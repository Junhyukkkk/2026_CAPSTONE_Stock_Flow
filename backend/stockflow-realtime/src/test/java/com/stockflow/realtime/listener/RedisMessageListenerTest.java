package com.stockflow.realtime.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.metrics.PipelineStageMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class RedisMessageListenerTest {

    private final SimpMessagingTemplate messagingTemplate = mock(SimpMessagingTemplate.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private RedisMessageListener listener(long sampleEvery) {
        return new RedisMessageListener(messagingTemplate, new PipelineStageMetrics(registry),
            new ObjectMapper(), registry, sampleEvery);
    }

    private static DefaultMessage message(String symbol) {
        String body = "{\"symbol\":\"" + symbol + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
        return new DefaultMessage(("price:" + symbol).getBytes(StandardCharsets.UTF_8),
            body.getBytes(StandardCharsets.UTF_8));
    }

    private static DefaultMessage message(String channel, String body) {
        return new DefaultMessage(channel.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }

    private long e2eCount() {
        return registry.get("stockflow.e2e.latency.websocket").summary().count();
    }

    // ---------- E2E 샘플링 ----------

    @Test
    void defaultSampleEvery1_measuresEveryMessage() {
        RedisMessageListener listener = listener(1);

        for (int i = 0; i < 100; i++) {
            listener.onMessage(message("BTC"), null);
        }

        assertThat(e2eCount()).isEqualTo(100);
        verify(messagingTemplate, times(100)).convertAndSend(eq("/topic/price/BTC"), anyString());
    }

    @Test
    void sampleEvery10_measuresOneInTenButBroadcastsAll() {
        RedisMessageListener listener = listener(10);

        for (int i = 0; i < 100; i++) {
            listener.onMessage(message("BTC"), null);
        }

        assertThat(e2eCount()).isEqualTo(10);
        verify(messagingTemplate, times(100)).convertAndSend(eq("/topic/price/BTC"), anyString());
    }

    @Test
    void sampleEveryZeroOrNegative_measuresEveryMessage() {
        RedisMessageListener listener = listener(0);

        for (int i = 0; i < 5; i++) {
            listener.onMessage(message("BTC"), null);
        }

        assertThat(e2eCount()).isEqualTo(5);
    }

    // ---------- 전달·예외 처리 ----------

    @Test
    void forwardsPriceMessageToWebSocketTopicAndTracksDispatchThread() {
        String body = "{\"symbol\":\"BTCUSDT\",\"timestamp\":" + (System.currentTimeMillis() - 30) + "}";

        listener(1).onMessage(message("price:BTCUSDT", body), null);

        verify(messagingTemplate).convertAndSend("/topic/price/BTCUSDT", body);
        assertThat(registry.get("stockflow.ws.dispatch.threads").gauge().value()).isEqualTo(1.0);
    }

    @Test
    void ignoresMessagesFromUnknownChannels() {
        listener(1).onMessage(message("other:BTC", "{}"), null);

        verify(messagingTemplate, never()).convertAndSend(anyString(), any(Object.class));
    }

    @Test
    void skipsLatencyWhenTimestampMissingOrBodyIsNotJson() {
        RedisMessageListener listener = listener(1);

        listener.onMessage(message("price:A", "{\"symbol\":\"A\"}"), null);
        listener.onMessage(message("price:B", "not-json"), null);

        verify(messagingTemplate).convertAndSend("/topic/price/A", "{\"symbol\":\"A\"}");
        verify(messagingTemplate).convertAndSend("/topic/price/B", "not-json");
        assertThat(e2eCount()).isZero();
    }

    @Test
    void sendFailureIsLoggedNotPropagated() {
        doThrow(new IllegalStateException("broker down")).when(messagingTemplate)
                .convertAndSend(anyString(), any(Object.class));

        listener(1).onMessage(message("price:A", "{}"), null);
    }
}
