package com.stockflow.realtime.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Redis Pub/Sub 메시지 리스너
 *
 * Redis 채널(price:{symbol})에서 메시지를 받아
 * WebSocket(/topic/price/{symbol})으로 전달
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisMessageListener implements MessageListener {

    private final SimpMessagingTemplate messagingTemplate;

    private static final String CHANNEL_PREFIX = "price:";
    private static final String WEBSOCKET_TOPIC_PREFIX = "/topic/price/";

    @Override
    public void onMessage(Message message, byte[] pattern) {
        try {
            String channel = new String(message.getChannel());
            String body = new String(message.getBody());

            // 채널에서 symbol 추출 (price:AAPL -> AAPL)
            String symbol = extractSymbol(channel);
            if (symbol == null) {
                log.warn("Invalid channel format: {}", channel);
                return;
            }

            // WebSocket으로 전달
            String destination = WEBSOCKET_TOPIC_PREFIX + symbol;
            messagingTemplate.convertAndSend(destination, body);

            log.trace("Forwarded to WebSocket: channel={}, destination={}", channel, destination);

        } catch (Exception e) {
            log.error("Failed to process Redis message", e);
        }
    }

    private String extractSymbol(String channel) {
        if (channel != null && channel.startsWith(CHANNEL_PREFIX)) {
            return channel.substring(CHANNEL_PREFIX.length());
        }
        return null;
    }
}
