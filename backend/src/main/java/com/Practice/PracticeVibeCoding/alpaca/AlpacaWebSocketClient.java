package com.Practice.PracticeVibeCoding.alpaca;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.util.concurrent.ExecutionException;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlpacaWebSocketClient {

    @Value("${alpaca.websocket.url}")
    private String websocketUrl;

    private final AlpacaWebSocketHandler handler;

    public void connect() {
        try {
            log.info("=== Alpaca WebSocket 연결 시도 ===");
            log.info("URL: {}", websocketUrl);

            StandardWebSocketClient client = new StandardWebSocketClient();
            client.execute(handler, new WebSocketHttpHeaders(), URI.create(websocketUrl)).get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("WebSocket 연결 실패: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void disconnect() {
        try {
            handler.disconnect();
            log.info("WebSocket 연결 종료 요청");
        } catch (Exception e) {
            log.error("연결 종료 실패: {}", e.getMessage());
        }
    }

    public boolean isConnected() {
        return handler.isConnected();
    }
}
