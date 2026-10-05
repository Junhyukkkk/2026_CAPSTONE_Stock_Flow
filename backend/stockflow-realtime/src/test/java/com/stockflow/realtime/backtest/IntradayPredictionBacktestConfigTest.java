package com.stockflow.realtime.backtest;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntradayPredictionBacktestConfigTest {

    @Test
    void defaultsWhenParamsMissing() {
        IntradayPredictionBacktestConfig config = IntradayPredictionBacktestConfig.from(null);

        assertThat(config.model()).isEqualTo("LOG_RETURN_ARIMA");
        assertThat(config.source()).isEqualTo("BINANCE");
        assertThat(config.warmup()).isEqualTo(50);
        assertThat(config.refitEvery()).isEqualTo(5);
        assertThat(config.maxHistory()).isEqualTo(200);
        assertThat(config.volatilityWindow()).isEqualTo(20);
        assertThat(config.volatilityMultiplier()).isEqualTo(0.5);
        assertThat(config.feeBps()).isEqualTo(10.0);
        assertThat(config.slippageBps()).isEqualTo(5.0);
    }

    @Test
    void parsesStringAndNumberValues() {
        IntradayPredictionBacktestConfig config = IntradayPredictionBacktestConfig.from(Map.of(
                "model", " arima ", "source", "alpaca", "warmup", "60", "refitEvery", 3,
                "maxHistory", 100, "volatilityWindow", "10", "volatilityMultiplier", "1.5",
                "feeBps", 2, "slippageBps", "1"));

        assertThat(config.model()).isEqualTo("ARIMA");
        assertThat(config.source()).isEqualTo("ALPACA");
        assertThat(config.warmup()).isEqualTo(60);
        assertThat(config.volatilityMultiplier()).isEqualTo(1.5);
        assertThat(config.asParams()).containsEntry("model", "ARIMA").containsEntry("refitEvery", 3).hasSize(9);
    }

    @Test
    void buildsRequestAndExecutionConfig() {
        IntradayPredictionBacktestConfig config = IntradayPredictionBacktestConfig.from(Map.of());
        Instant from = Instant.parse("2025-01-01T00:00:00Z");

        assertThat(config.toRequest("btcusdt", from, from.plusSeconds(60))).isNotNull();
        assertThat(config.executionConfig().feeRate()).isEqualByComparingTo("0.001");
        assertThat(config.executionConfig().slippageRate()).isEqualByComparingTo("0.0005");
    }

    @Test
    void rejectsInvalidValues() {
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("model", "PROPHET")))
                .hasMessageContaining("model must be");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("warmup", 10)))
                .hasMessageContaining("warmup");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("refitEvery", 99)))
                .hasMessageContaining("refitEvery");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("maxHistory", 40)))
                .hasMessageContaining("maxHistory");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("volatilityWindow", 1)))
                .hasMessageContaining("volatilityWindow");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("volatilityMultiplier", 9)))
                .hasMessageContaining("volatilityMultiplier");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("feeBps", -1)))
                .hasMessageContaining("feeBps");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("warmup", "abc")))
                .hasMessageContaining("must be an integer");
        assertThatThrownBy(() -> IntradayPredictionBacktestConfig.from(Map.of("feeBps", "abc")))
                .hasMessageContaining("must be a number");
    }
}
