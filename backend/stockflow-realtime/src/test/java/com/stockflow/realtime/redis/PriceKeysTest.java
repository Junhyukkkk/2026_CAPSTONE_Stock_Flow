package com.stockflow.realtime.redis;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 설계 문서 6.1(캐시 Key)·7.1(Pub/Sub 채널)·7.2(WebSocket 목적지)에 정의된 이름 규칙을 고정한다.
 * 이 값은 프론트엔드(ui/*.html 의 STOMP 구독)와 운영 스크립트가 그대로 의존한다.
 */
class PriceKeysTest {

    @Test
    void keyAndChannelNamesFollowTheDesignDoc() {
        assertThat(PriceKeys.latestPrice("AAPL")).isEqualTo("price:latest:AAPL");
        assertThat(PriceKeys.prevClose("AAPL")).isEqualTo("price:prev-close:AAPL");
        assertThat(PriceKeys.priceChannel("AAPL")).isEqualTo("price:AAPL");
        assertThat(PriceKeys.wsPriceTopic("AAPL")).isEqualTo("/topic/price/AAPL");
        assertThat(PriceKeys.PRICE_CHANNEL_PATTERN).isEqualTo("price:*");
    }

    @Test
    void ttlsFollowTheDesignDoc() {
        assertThat(PriceKeys.LATEST_PRICE_TTL).isEqualTo(Duration.ofSeconds(60));
        assertThat(PriceKeys.PREV_CLOSE_TTL).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void symbolFromChannel_stripsPrefixOrReturnsNull() {
        assertThat(PriceKeys.symbolFromChannel("price:BTCUSDT")).isEqualTo("BTCUSDT");
        assertThat(PriceKeys.symbolFromChannel("other:BTCUSDT")).isNull();
        assertThat(PriceKeys.symbolFromChannel(null)).isNull();
    }
}
