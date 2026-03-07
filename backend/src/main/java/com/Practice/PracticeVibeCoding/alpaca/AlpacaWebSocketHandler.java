package com.Practice.PracticeVibeCoding.alpaca;

import com.Practice.PracticeVibeCoding.alpaca.dto.AlpacaQuoteMessage;
import com.Practice.PracticeVibeCoding.alpaca.dto.AlpacaTradeMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.List;

@Slf4j
@Component
public class AlpacaWebSocketHandler extends TextWebSocketHandler {

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private WebSocketSession currentSession;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("=== Alpaca WebSocket 연결 성공 ===");
        this.currentSession = session;
        sendAuthMessage(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        log.info("수신된 메시지: {}", payload);

        JsonNode rootNode = objectMapper.readTree(payload);

        // Alpaca는 배열로 메시지를 보냄
        if (rootNode.isArray()) {
            for (JsonNode node : rootNode) {
                handleSingleMessage(session, node);
            }
        }
    }

    private void handleSingleMessage(WebSocketSession session, JsonNode node) throws IOException {
        String messageType = node.has("T") ? node.get("T").asText() : "";

        switch (messageType) {
            case "success":
                handleSuccessMessage(session, node);
                break;
            case "error":
                handleErrorMessage(node);
                break;
            case "t":
                handleTradeMessage(node);
                break;
            case "q":
                handleQuoteMessage(node);
                break;
            case "subscription":
                handleSubscriptionMessage(node);
                break;
            default:
                log.info("알 수 없는 메시지 타입: {}", messageType);
        }
    }

    private void handleSuccessMessage(WebSocketSession session, JsonNode node) throws IOException {
        String msg = node.has("msg") ? node.get("msg").asText() : "";
        log.info("성공 메시지: {}", msg);

        if ("authenticated".equals(msg)) {
            log.info("=== 인증 성공! 구독 시작 ===");
            subscribeToSymbols(session, List.of("AAPL", "MSFT", "GOOGL"));
        }
    }

    private void handleErrorMessage(JsonNode node) {
        String msg = node.has("msg") ? node.get("msg").asText() : "Unknown error";
        int code = node.has("code") ? node.get("code").asInt() : -1;
        log.error("에러 발생 - 코드: {}, 메시지: {}", code, msg);
    }

    private void handleTradeMessage(JsonNode node) throws JsonProcessingException {
        AlpacaTradeMessage trade = objectMapper.treeToValue(node, AlpacaTradeMessage.class);
        log.info("=== 체결 데이터 ===");
        log.info("종목: {} | 가격: ${} | 수량: {} | 시간: {}",
                trade.getSymbol(),
                trade.getPrice(),
                trade.getSize(),
                trade.getTimestamp());
    }

    private void handleQuoteMessage(JsonNode node) throws JsonProcessingException {
        AlpacaQuoteMessage quote = objectMapper.treeToValue(node, AlpacaQuoteMessage.class);
        log.info("=== 호가 데이터 ===");
        log.info("종목: {} | 매수: ${} x {} | 매도: ${} x {}",
                quote.getSymbol(),
                quote.getBidPrice(),
                quote.getBidSize(),
                quote.getAskPrice(),
                quote.getAskSize());
    }

    private void handleSubscriptionMessage(JsonNode node) {
        log.info("=== 구독 완료 ===");
        if (node.has("trades")) {
            log.info("체결 구독: {}", node.get("trades"));
        }
        if (node.has("quotes")) {
            log.info("호가 구독: {}", node.get("quotes"));
        }
    }

    private void sendAuthMessage(WebSocketSession session) throws IOException {
        String authMessage = String.format(
                "{\"action\":\"auth\",\"key\":\"%s\",\"secret\":\"%s\"}",
                apiKey, apiSecret
        );
        session.sendMessage(new TextMessage(authMessage));
        log.info("인증 메시지 전송 완료");
    }

    private void subscribeToSymbols(WebSocketSession session, List<String> symbols) throws IOException {
        String symbolsJson = objectMapper.writeValueAsString(symbols);
        String subscribeMessage = String.format(
                "{\"action\":\"subscribe\",\"trades\":%s,\"quotes\":%s}",
                symbolsJson, symbolsJson
        );
        session.sendMessage(new TextMessage(subscribeMessage));
        log.info("구독 요청 전송: {}", symbols);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("WebSocket 전송 에러: {}", exception.getMessage());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.info("=== Alpaca WebSocket 연결 종료 === 상태: {}", status);
        this.currentSession = null;
    }

    public boolean isConnected() {
        return currentSession != null && currentSession.isOpen();
    }

    public void disconnect() throws IOException {
        if (currentSession != null && currentSession.isOpen()) {
            currentSession.close();
        }
    }
}
