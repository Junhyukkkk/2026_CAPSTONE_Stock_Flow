package com.stockflow.realtime.backtest;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PredictionBacktestConfigTest {

    @Test
    void rejectsWarmupBelowFiftyObservations() {
        assertThrows(IllegalArgumentException.class,
                () -> PredictionBacktestConfig.from(Map.of("warmup", 49)));
    }

    @Test
    void acceptsFiftyObservations() {
        assertDoesNotThrow(
                () -> PredictionBacktestConfig.from(Map.of("warmup", 50)));
    }
}
