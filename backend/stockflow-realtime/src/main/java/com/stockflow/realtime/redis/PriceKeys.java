package com.stockflow.realtime.redis;

import java.time.Duration;

/**
 * 실시간 가격 캐시·브로드캐스트에 쓰는 Redis 키, Pub/Sub 채널, WebSocket 목적지 이름 규칙.
 *
 * 설계 문서 "6.1 캐시 Key 설계 / 7.1 Pub/Sub 채널 / 7.2 전체 처리 흐름":
 * <pre>
 *   price:latest:{symbol}      String(JSON PriceSnapshot), TTL 60초
 *   price:prev-close:{symbol}  String(전일 종가),          TTL 24시간
 *   price:{symbol}             Pub/Sub 채널 (RedisPriceService → RedisMessageListener)
 *   /topic/price/{symbol}      STOMP 목적지 (RedisMessageListener → 브라우저)
 * </pre>
 * 세 컴포넌트(RedisPriceService, RedisMessageListener, RedisConfig)가 같은 문자열을 따로 들고
 * 있으면 한 곳만 바뀌어도 조용히 끊기므로 여기서만 정의한다.
 */
public final class PriceKeys {

    public static final String LATEST_PRICE_PREFIX = "price:latest:";
    public static final String PREV_CLOSE_PREFIX = "price:prev-close:";
    public static final String PRICE_CHANNEL_PREFIX = "price:";
    /** RedisMessageListenerContainer 가 구독하는 패턴 (price:AAPL, price:BTCUSDT …) */
    public static final String PRICE_CHANNEL_PATTERN = PRICE_CHANNEL_PREFIX + "*";
    public static final String WS_PRICE_TOPIC_PREFIX = "/topic/price/";

    public static final Duration LATEST_PRICE_TTL = Duration.ofSeconds(60);
    public static final Duration PREV_CLOSE_TTL = Duration.ofHours(24);

    private PriceKeys() {
    }

    public static String latestPrice(String symbol) {
        return LATEST_PRICE_PREFIX + symbol;
    }

    public static String prevClose(String symbol) {
        return PREV_CLOSE_PREFIX + symbol;
    }

    public static String priceChannel(String symbol) {
        return PRICE_CHANNEL_PREFIX + symbol;
    }

    public static String wsPriceTopic(String symbol) {
        return WS_PRICE_TOPIC_PREFIX + symbol;
    }

    /** Pub/Sub 채널 이름에서 심볼을 꺼낸다. 규칙에 맞지 않는 채널이면 null. */
    public static String symbolFromChannel(String channel) {
        if (channel != null && channel.startsWith(PRICE_CHANNEL_PREFIX)) {
            return channel.substring(PRICE_CHANNEL_PREFIX.length());
        }
        return null;
    }
}
