package com.Practice.PracticeVibeCoding.alpaca;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHttpHeaders;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.util.concurrent.ExecutionException;

/**
 * IEX 거래소의 모든 종목 데이터를 수신하는 WebSocket 클라이언트
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AlpacaAllStocksClient {

    @Value("${alpaca.websocket.url}")
    private String websocketUrl;

    private final AlpacaAllStocksHandler handler;

    public void connect() {
        try {
            log.info("=== [ALL STOCKS] WebSocket 연결 시도 ===");
            log.info("URL: {}", websocketUrl);

            StandardWebSocketClient client = new StandardWebSocketClient();
            client.execute(handler, new WebSocketHttpHeaders(), URI.create(websocketUrl)).get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("[ALL STOCKS] WebSocket 연결 실패: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    public void disconnect() {
        try {
            handler.disconnect();
            log.info("[ALL STOCKS] WebSocket 연결 종료 요청");
        } catch (Exception e) {
            log.error("[ALL STOCKS] 연결 종료 실패: {}", e.getMessage());
        }
    }

    public boolean isConnected() {
        return handler.isConnected();
    }

    public long getTradeCount() {
        return handler.getTradeCount();
    }

    public long getQuoteCount() {
        return handler.getQuoteCount();
    }
}
