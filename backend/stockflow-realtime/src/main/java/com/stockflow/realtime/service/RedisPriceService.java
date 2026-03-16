package com.stockflow.realtime.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.dto.PriceSnapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * Redis 가격 서비스
 *
 * 역할:
 * - 최신 체결가 캐싱 (price:latest:{symbol})
 * - 전일 종가 관리 (price:prev-close:{symbol})
 * - 등락률 계산
 * - Pub/Sub 발행 (price:{symbol})
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RedisPriceService {

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    // Redis Key Prefix
    private static final String KEY_LATEST_PRICE = "price:latest:";
    private static final String KEY_PREV_CLOSE = "price:prev-close:";

    // Pub/Sub Channel Prefix
    private static final String CHANNEL_PRICE = "price:";

    // TTL 설정
    private static final Duration TTL_LATEST_PRICE = Duration.ofSeconds(60);
    private static final Duration TTL_PREV_CLOSE = Duration.ofHours(24);

    /**
     * 실시간 거래 데이터 처리
     *
     * 1. 전일 종가 조회
     * 2. 등락률 계산
     * 3. 최신가 캐싱
     * 4. Pub/Sub 발행
     */
    public void processRealtimeTrade(NormalizedTradeDTO trade) {
        String symbol = trade.getSymbol();
        BigDecimal currentPrice = trade.getPrice();

        // 1. 전일 종가 조회
        BigDecimal prevClose = getPreviousClose(symbol);

        // 2. 등락률 계산
        BigDecimal change = BigDecimal.ZERO;
        BigDecimal changePercent = BigDecimal.ZERO;

        if (prevClose != null && prevClose.compareTo(BigDecimal.ZERO) > 0) {
            change = currentPrice.subtract(prevClose);
            changePercent = change.divide(prevClose, 4, RoundingMode.HALF_UP)
                    .multiply(BigDecimal.valueOf(100))
                    .setScale(2, RoundingMode.HALF_UP);
        }

        // 3. PriceSnapshot 생성
        PriceSnapshot snapshot = PriceSnapshot.builder()
                .symbol(symbol)
                .price(currentPrice)
                .volume(trade.getVolume())
                .exchange(trade.getExchange())
                .timestamp(trade.getTimestamp())
                .change(change)
                .changePercent(changePercent)
                .marketType(trade.getMarketType())
                .build();

        // 4. Redis 캐싱 + Pub/Sub 발행
        saveLatestPrice(symbol, snapshot);
        publishPriceUpdate(symbol, snapshot);

        log.debug("Processed realtime trade: symbol={}, price={}, change={}, changePercent={}%",
                symbol, currentPrice, change, changePercent);
    }

    /**
     * 최신가 캐싱
     */
    private void saveLatestPrice(String symbol, PriceSnapshot snapshot) {
        try {
            String key = KEY_LATEST_PRICE + symbol;
            String value = objectMapper.writeValueAsString(snapshot);
            redisTemplate.opsForValue().set(key, value, TTL_LATEST_PRICE);
            log.trace("Cached latest price: key={}", key);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize PriceSnapshot: symbol={}", symbol, e);
            throw new RuntimeException("Failed to cache latest price", e);
        }
    }

    /**
     * Pub/Sub 발행
     */
    private void publishPriceUpdate(String symbol, PriceSnapshot snapshot) {
        try {
            String channel = CHANNEL_PRICE + symbol;
            String message = objectMapper.writeValueAsString(snapshot);
            redisTemplate.convertAndSend(channel, message);
            log.trace("Published price update: channel={}", channel);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize PriceSnapshot for publish: symbol={}", symbol, e);
        }
    }

    /**
     * 최신가 조회
     */
    public PriceSnapshot getLatestPrice(String symbol) {
        try {
            String key = KEY_LATEST_PRICE + symbol;
            String value = redisTemplate.opsForValue().get(key);
            if (value == null) {
                return null;
            }
            return objectMapper.readValue(value, PriceSnapshot.class);
        } catch (JsonProcessingException e) {
            log.error("Failed to deserialize PriceSnapshot: symbol={}", symbol, e);
            return null;
        }
    }

    /**
     * 전일 종가 조회
     */
    public BigDecimal getPreviousClose(String symbol) {
        String key = KEY_PREV_CLOSE + symbol;
        String value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        return new BigDecimal(value);
    }

    /**
     * 전일 종가 설정
     * (배치 작업이나 장 마감 시 호출)
     */
    public void setPreviousClose(String symbol, BigDecimal price) {
        String key = KEY_PREV_CLOSE + symbol;
        redisTemplate.opsForValue().set(key, price.toPlainString(), TTL_PREV_CLOSE);
        log.info("Set previous close: symbol={}, price={}", symbol, price);
    }
}
