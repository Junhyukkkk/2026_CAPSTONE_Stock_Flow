package com.stockflow.realtime.test;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 테스트 메시지 전송용 컨트롤러
 *
 * 정상/비정상 메시지를 Kafka로 전송하여 Consumer 테스트에 활용
 * test 프로파일에서만 활성화
 */
@Slf4j
@RestController
@RequestMapping("/test")
@Profile("test")
@RequiredArgsConstructor
public class TestMessageProducer {

    private final KafkaTemplate<String, NormalizedTradeDTO> kafkaTemplate;

    @Value("${spring.kafka.topic.normalized:market.normalized.test}")
    private String normalizedTopic;

    private static final String[] SYMBOLS = {"BTCUSDT", "ETHUSDT", "AAPL", "GOOGL", "MSFT"};
    private static final String[] SOURCES = {"BINANCE", "ALPACA"};
    private static final String[] EXCHANGES = {"BINANCE", "IEX", "NYSE"};
    private static final String[] MARKET_TYPES = {"CRYPTO", "STOCK"};

    @GetMapping("/send")
    public Map<String, Object> sendValidMessages(@RequestParam(defaultValue = "1000") int count) {
        log.info("Sending {} valid messages to topic: {}", count, normalizedTopic);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            NormalizedTradeDTO trade = createValidMessage(i);

            CompletableFuture<SendResult<String, NormalizedTradeDTO>> future =
                kafkaTemplate.send(normalizedTopic, trade.getSymbol(), trade);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    failCount.incrementAndGet();
                    log.error("Failed to send message: {}", ex.getMessage());
                } else {
                    successCount.incrementAndGet();
                }
            });
        }

        long elapsed = System.currentTimeMillis() - startTime;

        Map<String, Object> response = new HashMap<>();
        response.put("topic", normalizedTopic);
        response.put("requestedCount", count);
        response.put("elapsedMs", elapsed);
        response.put("message", "Messages are being sent asynchronously");

        log.info("Initiated sending {} messages in {}ms", count, elapsed);
        return response;
    }

    @GetMapping("/send-invalid")
    public Map<String, Object> sendInvalidMessages(@RequestParam(defaultValue = "100") int count) {
        log.info("Sending {} invalid messages to topic: {}", count, normalizedTopic);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            NormalizedTradeDTO trade = createInvalidMessage(i);

            CompletableFuture<SendResult<String, NormalizedTradeDTO>> future =
                kafkaTemplate.send(normalizedTopic, trade.getSymbol(), trade);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    failCount.incrementAndGet();
                    log.error("Failed to send message: {}", ex.getMessage());
                } else {
                    successCount.incrementAndGet();
                }
            });
        }

        long elapsed = System.currentTimeMillis() - startTime;

        Map<String, Object> response = new HashMap<>();
        response.put("topic", normalizedTopic);
        response.put("requestedCount", count);
        response.put("invalidType", "price is null or negative");
        response.put("elapsedMs", elapsed);
        response.put("message", "Invalid messages are being sent asynchronously");

        log.info("Initiated sending {} invalid messages in {}ms", count, elapsed);
        return response;
    }

    private NormalizedTradeDTO createValidMessage(int index) {
        int symbolIdx = index % SYMBOLS.length;
        String symbol = SYMBOLS[symbolIdx];
        boolean isCrypto = symbol.endsWith("USDT");

        return NormalizedTradeDTO.builder()
                .tradeId(UUID.randomUUID().toString())
                .source(isCrypto ? "BINANCE" : "ALPACA")
                .symbol(symbol)
                .price(generateRandomPrice(symbol))
                .volume(generateRandomVolume())
                .exchange(isCrypto ? "BINANCE" : EXCHANGES[index % 2 + 1])
                .timestamp(System.currentTimeMillis())
                .receivedAt(System.currentTimeMillis())
                .marketType(isCrypto ? "CRYPTO" : "STOCK")
                .build();
    }

    private NormalizedTradeDTO createInvalidMessage(int index) {
        int symbolIdx = index % SYMBOLS.length;
        String symbol = SYMBOLS[symbolIdx];
        boolean isCrypto = symbol.endsWith("USDT");

        BigDecimal invalidPrice;
        if (index % 2 == 0) {
            invalidPrice = null;  // null price
        } else {
            invalidPrice = BigDecimal.valueOf(-100.0 - index);  // negative price
        }

        return NormalizedTradeDTO.builder()
                .tradeId(UUID.randomUUID().toString())
                .source(isCrypto ? "BINANCE" : "ALPACA")
                .symbol(symbol)
                .price(invalidPrice)
                .volume(generateRandomVolume())
                .exchange(isCrypto ? "BINANCE" : EXCHANGES[index % 2 + 1])
                .timestamp(System.currentTimeMillis())
                .receivedAt(System.currentTimeMillis())
                .marketType(isCrypto ? "CRYPTO" : "STOCK")
                .build();
    }

    private BigDecimal generateRandomPrice(String symbol) {
        double basePrice = switch (symbol) {
            case "BTCUSDT" -> 65000.0;
            case "ETHUSDT" -> 3500.0;
            case "AAPL" -> 180.0;
            case "GOOGL" -> 140.0;
            case "MSFT" -> 420.0;
            default -> 100.0;
        };
        double variance = basePrice * 0.001 * (Math.random() - 0.5);
        return BigDecimal.valueOf(basePrice + variance);
    }

    private BigDecimal generateRandomVolume() {
        return BigDecimal.valueOf(0.1 + Math.random() * 10.0);
    }
}
