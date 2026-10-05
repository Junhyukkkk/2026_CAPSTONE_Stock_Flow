package com.stockflow.realtime.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.metrics.PipelineStageMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class RedisMessageListenerTest {

    SimpMessagingTemplate messagingTemplate;
    SimpleMeterRegistry registry;
    RedisMessageListener listener;

    @BeforeEach
    void setUp() {
        messagingTemplate = mock(SimpMessagingTemplate.class);
        registry = new SimpleMeterRegistry();
        listener = new RedisMessageListener(messagingTemplate, new PipelineStageMetrics(registry),
                new ObjectMapper(), registry);
    }

    private static DefaultMessage message(String channel, String body) {
        return new DefaultMessage(channel.getBytes(StandardCharsets.UTF_8), body.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void forwardsPriceMessageToWebSocketTopicAndRecordsLatency() {
        String body = "{\"symbol\":\"BTCUSDT\",\"timestamp\":" + (System.currentTimeMillis() - 30) + "}";

        listener.onMessage(message("price:BTCUSDT", body), null);

        verify(messagingTemplate).convertAndSend("/topic/price/BTCUSDT", body);
        assertThat(registry.get("stockflow.e2e.latency.websocket").summary().count()).isEqualTo(1);
        assertThat(registry.get("stockflow.ws.dispatch.threads").gauge().value()).isEqualTo(1.0);
    }

    @Test
    void ignoresMessagesFromUnknownChannels() {
        listener.onMessage(message("other:BTC", "{}"), null);

        verify(messagingTemplate, never()).convertAndSend(anyString(), any(Object.class));
    }

    @Test
    void skipsLatencyWhenTimestampMissingOrBodyIsNotJson() {
        listener.onMessage(message("price:A", "{\"symbol\":\"A\"}"), null);
        listener.onMessage(message("price:B", "not-json"), null);

        verify(messagingTemplate).convertAndSend("/topic/price/A", "{\"symbol\":\"A\"}");
        verify(messagingTemplate).convertAndSend("/topic/price/B", "not-json");
        assertThat(registry.get("stockflow.e2e.latency.websocket").summary().count()).isZero();
    }

    @Test
    void sendFailureIsLoggedNotPropagated() {
        doThrow(new IllegalStateException("broker down")).when(messagingTemplate)
                .convertAndSend(anyString(), any(Object.class));

        listener.onMessage(message("price:A", "{}"), null);
    }
}
