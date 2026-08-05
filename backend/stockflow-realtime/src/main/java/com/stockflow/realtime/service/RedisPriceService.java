package com.stockflow.realtime.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.core.metrics.PipelineStageMetrics;
import com.stockflow.core.metrics.PipelineStageMetrics.Stage;
import com.stockflow.realtime.config.OptimizationProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private final PipelineStageMetrics stageMetrics;
    private final OptimizationProperties opt;

    /**
     * 전일 종가 로컬 캐시. 하루 한 번 변하는 값이라 매 메시지 조회할 이유가 없다.
     * 값이 없는 경우(miss)도 캐싱해야 효과가 있다 — 종가 적재 배치가 없으면
     * 모든 조회가 miss 이고 그 miss 마다 Redis 왕복이 발생한다.
     */
    private record CachedPrevClose(BigDecimal value, long cachedAt) {
    }

    private final Map<String, CachedPrevClose> prevCloseCache = new ConcurrentHashMap<>();

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
        // Price validation
        if (trade.getPrice() == null || trade.getPrice().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Invalid price: " + trade.getPrice());
        }

        long totalStart = stageMetrics.start();

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
        if (opt.isRedisPipeline()) {
            saveAndPublish(symbol, snapshot);
        } else {
            saveLatestPrice(symbol, snapshot);
            publishPriceUpdate(symbol, snapshot);
        }

        stageMetrics.record(Stage.REALTIME_TOTAL, totalStart);

        log.debug("Processed realtime trade: symbol={}, price={}, change={}, changePercent={}%",
                symbol, currentPrice, change, changePercent);
    }

    /**
     * 최신가 캐싱
     */
    private void saveLatestPrice(String symbol, PriceSnapshot snapshot) {
        try {
            String key = KEY_LATEST_PRICE + symbol;

            long serializeStart = stageMetrics.start();
            String value = objectMapper.writeValueAsString(snapshot);
            stageMetrics.record(Stage.SNAPSHOT_SERIALIZE, serializeStart);

            long setStart = stageMetrics.start();
            redisTemplate.opsForValue().set(key, value, TTL_LATEST_PRICE);
            stageMetrics.record(Stage.REDIS_SET_LATEST, setStart);

            log.trace("Cached latest price: key={}", key);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize PriceSnapshot: symbol={}", symbol, e);
            throw new RuntimeException("Failed to cache latest price", e);
        }
    }

    /**
     * 최신가 캐싱 + Pub/Sub 발행을 파이프라인 1회 왕복으로 처리
     *
     * 직렬화도 1회만 수행한다 (기존 경로는 동일 snapshot 을 두 번 직렬화한다).
     */
    private void saveAndPublish(String symbol, PriceSnapshot snapshot) {
        try {
            long serializeStart = stageMetrics.start();
            String payload = objectMapper.writeValueAsString(snapshot);
            stageMetrics.record(Stage.SNAPSHOT_SERIALIZE, serializeStart);

            byte[] key = (KEY_LATEST_PRICE + symbol).getBytes(StandardCharsets.UTF_8);
            byte[] channel = (CHANNEL_PRICE + symbol).getBytes(StandardCharsets.UTF_8);
            byte[] value = payload.getBytes(StandardCharsets.UTF_8);

            long pipelineStart = stageMetrics.start();
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                connection.stringCommands().set(
                        key, value,
                        Expiration.from(TTL_LATEST_PRICE),
                        RedisStringCommands.SetOption.upsert());
                connection.publish(channel, value);
                return null;
            });
            stageMetrics.record(Stage.REDIS_SET_PUBLISH_PIPELINED, pipelineStart);

            log.trace("Pipelined set+publish: symbol={}", symbol);
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

            // saveLatestPrice와 동일한 snapshot을 다시 직렬화한다 (중복 비용 계측용)
            long serializeStart = stageMetrics.start();
            String message = objectMapper.writeValueAsString(snapshot);
            stageMetrics.record(Stage.SNAPSHOT_SERIALIZE, serializeStart);

            long publishStart = stageMetrics.start();
            redisTemplate.convertAndSend(channel, message);
            stageMetrics.record(Stage.REDIS_PUBLISH, publishStart);

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
        if (opt.isPrevCloseLocalCache()) {
            CachedPrevClose cached = prevCloseCache.get(symbol);
            if (cached != null
                    && System.currentTimeMillis() - cached.cachedAt() < opt.getPrevCloseLocalCacheTtlMs()) {
                return cached.value();
            }
        }

        String key = KEY_PREV_CLOSE + symbol;

        long getStart = stageMetrics.start();
        String value = redisTemplate.opsForValue().get(key);
        stageMetrics.record(Stage.REDIS_PREV_CLOSE_GET, getStart);

        BigDecimal result = value == null ? null : new BigDecimal(value);

        if (opt.isPrevCloseLocalCache()) {
            prevCloseCache.put(symbol, new CachedPrevClose(result, System.currentTimeMillis()));
        }
        return result;
    }

    /**
     * 전일 종가 설정
     * (배치 작업이나 장 마감 시 호출)
     */
    public void setPreviousClose(String symbol, BigDecimal price) {
        String key = KEY_PREV_CLOSE + symbol;
        redisTemplate.opsForValue().set(key, price.toPlainString(), TTL_PREV_CLOSE);
        prevCloseCache.remove(symbol);   // 로컬 캐시가 옛 값을 붙들지 않도록 무효화
        log.info("Set previous close: symbol={}, price={}", symbol, price);
    }

    /**
     * 전일 종가 일괄 적재 (배치 전용)
     *
     * 다수 종목을 한 번에 채운다. 하루 한 번 도는 배치라 hot path 가 아니므로
     * 파이프라인 대신 단순 루프로 처리한다(커넥션 풀 미비로 파이프라인이 오히려 느림).
     * 심볼당 INFO 로그는 남기지 않고 총계만 반환한다.
     *
     * @param closes symbol -> 전일 종가
     * @return 적재한 종목 수
     */
    public int loadPreviousCloses(java.util.Map<String, BigDecimal> closes) {
        for (java.util.Map.Entry<String, BigDecimal> e : closes.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }
            redisTemplate.opsForValue().set(
                    KEY_PREV_CLOSE + e.getKey(), e.getValue().toPlainString(), TTL_PREV_CLOSE);
            prevCloseCache.remove(e.getKey());
        }
        log.info("Loaded previous closes into Redis: count={}", closes.size());
        return closes.size();
    }
}
