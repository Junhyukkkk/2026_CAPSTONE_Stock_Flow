package com.stockflow.realtime.redis;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PriceKeysMappingTest {

    @Test
    void buildsKeysChannelsAndTopics() {
        assertThat(PriceKeys.latestPrice("BTC")).isEqualTo("price:latest:BTC");
        assertThat(PriceKeys.prevClose("BTC")).isEqualTo("price:prev-close:BTC");
        assertThat(PriceKeys.priceChannel("BTC")).isEqualTo("price:BTC");
        assertThat(PriceKeys.wsPriceTopic("BTC")).isEqualTo("/topic/price/BTC");
    }

    @Test
    void symbolFromChannelStripsPrefixOrReturnsNull() {
        assertThat(PriceKeys.symbolFromChannel("price:BTC")).isEqualTo("BTC");
        assertThat(PriceKeys.symbolFromChannel("other:BTC")).isNull();
        assertThat(PriceKeys.symbolFromChannel(null)).isNull();
    }
}
