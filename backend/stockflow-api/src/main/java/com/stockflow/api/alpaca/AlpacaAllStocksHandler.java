package com.stockflow.api.alpaca;

import com.stockflow.api.alpaca.dto.AlpacaQuoteMessage;
import com.stockflow.api.alpaca.dto.AlpacaTradeMessage;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * IEX 거래소의 모든 종목 데이터를 구독하는 WebSocket 핸들러
 * 와일드카드(*)를 사용하여 전체 종목의 체결/호가 데이터 수신
 */
@Slf4j
@Component
public class AlpacaAllStocksHandler extends TextWebSocketHandler {

    @Value("${alpaca.api.key}")
    private String apiKey;

    @Value("${alpaca.api.secret}")
    private String apiSecret;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private WebSocketSession currentSession;

    private final AtomicLong tradeCount = new AtomicLong(0);
    private final AtomicLong quoteCount = new AtomicLong(0);
    private long startTime;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("=== [ALL STOCKS] Alpaca WebSocket 연결 성공 ===");
        this.currentSession = session;
        this.startTime = System.currentTimeMillis();
        tradeCount.set(0);
        quoteCount.set(0);
        sendAuthMessage(session);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();

        JsonNode rootNode = objectMapper.readTree(payload);

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
                log.debug("알 수 없는 메시지 타입: {}", messageType);
        }
    }

    private void handleSuccessMessage(WebSocketSession session, JsonNode node) throws IOException {
        String msg = node.has("msg") ? node.get("msg").asText() : "";
        log.info("[ALL STOCKS] 성공 메시지: {}", msg);

        if ("authenticated".equals(msg)) {
            log.info("=== [ALL STOCKS] 인증 성공! 전체 종목 구독 시작 ===");
            subscribeToAllStocks(session);
        }
    }

    private void handleErrorMessage(JsonNode node) {
        String msg = node.has("msg") ? node.get("msg").asText() : "Unknown error";
        int code = node.has("code") ? node.get("code").asInt() : -1;
        log.error("[ALL STOCKS] 에러 발생 - 코드: {}, 메시지: {}", code, msg);
    }

    private void handleTradeMessage(JsonNode node) throws JsonProcessingException {
        AlpacaTradeMessage trade = objectMapper.treeToValue(node, AlpacaTradeMessage.class);
        long count = tradeCount.incrementAndGet();

        if (count <= 10 || count % 100 == 0) {
            log.info("[TRADE #{}] {} | ${} | {} shares",
                    count,
                    trade.getSymbol(),
                    trade.getPrice(),
                    trade.getSize());
        }

        if (count % 1000 == 0) {
            printStats();
        }
    }

    private void handleQuoteMessage(JsonNode node) throws JsonProcessingException {
        AlpacaQuoteMessage quote = objectMapper.treeToValue(node, AlpacaQuoteMessage.class);
        long count = quoteCount.incrementAndGet();

        if (count <= 5 || count % 1000 == 0) {
            log.info("[QUOTE #{}] {} | Bid: ${} x {} | Ask: ${} x {}",
                    count,
                    quote.getSymbol(),
                    quote.getBidPrice(),
                    quote.getBidSize(),
                    quote.getAskPrice(),
                    quote.getAskSize());
        }
    }

    private void handleSubscriptionMessage(JsonNode node) {
        log.info("=== [ALL STOCKS] 구독 완료 ===");
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
        log.info("[ALL STOCKS] 인증 메시지 전송 완료");
    }

    private void subscribeToAllStocks(WebSocketSession session) throws IOException {
        String subscribeMessage = "{\"action\":\"subscribe\",\"trades\":[\"*\"],\"quotes\":[\"*\"]}";
        session.sendMessage(new TextMessage(subscribeMessage));
        log.info("[ALL STOCKS] 전체 종목 구독 요청 전송 (trades: *, quotes: *)");
    }

    private void printStats() {
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        log.info("=== [통계] 경과: {}초 | 체결: {}건 | 호가: {}건 ===",
                elapsed, tradeCount.get(), quoteCount.get());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("[ALL STOCKS] WebSocket 전송 에러: {}", exception.getMessage());
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        log.info("=== [ALL STOCKS] WebSocket 연결 종료 === 상태: {}", status);
        printStats();
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

    public long getTradeCount() {
        return tradeCount.get();
    }

    public long getQuoteCount() {
        return quoteCount.get();
    }
}
