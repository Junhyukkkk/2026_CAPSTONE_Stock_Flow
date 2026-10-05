package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.Signal;
import com.stockflow.realtime.backtest.model.StrategyType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StrategiesTest {

    private static List<Bar> bars(double... closes) {
        List<Bar> bars = new ArrayList<>();
        for (int i = 0; i < closes.length; i++) {
            BigDecimal c = BigDecimal.valueOf(closes[i]);
            bars.add(new Bar(LocalDate.of(2025, 1, 1).plusDays(i), c, c, c, c, BigDecimal.ONE));
        }
        return bars;
    }

    // ----- BuyAndHold -----

    @Test
    void buyAndHoldBuysOnlyOnFirstBar() {
        assertThat(new BuyAndHoldStrategy().generateSignals(bars(1, 2, 3)))
                .containsExactly(Signal.BUY, Signal.HOLD, Signal.HOLD);
        assertThat(new BuyAndHoldStrategy().generateSignals(List.of())).isEmpty();
    }

    // ----- MA crossover -----

    @Test
    void maCrossoverRejectsInvalidPeriods() {
        assertThatThrownBy(() -> new MaCrossoverStrategy(0, 5)).hasMessageContaining(">= 1");
        assertThatThrownBy(() -> new MaCrossoverStrategy(5, 0)).hasMessageContaining(">= 1");
        assertThatThrownBy(() -> new MaCrossoverStrategy(5, 5)).hasMessageContaining("shortPeriod must be < longPeriod");
    }

    @Test
    void maCrossoverEmitsGoldenAndDeadCrosses() {
        // 하락 → 급등(골든크로스) → 급락(데드크로스)
        List<Signal> signals = new MaCrossoverStrategy(2, 4)
                .generateSignals(bars(10, 9, 8, 7, 6, 20, 30, 40, 10, 5, 1));

        assertThat(signals).hasSize(11);
        assertThat(signals).contains(Signal.BUY, Signal.SELL);
        assertThat(signals.subList(0, 4)).containsOnly(Signal.HOLD); // longPeriod 이전은 HOLD
        assertThat(signals.indexOf(Signal.BUY)).isLessThan(signals.indexOf(Signal.SELL));
    }

    @Test
    void maCrossoverHoldsOnFlatSeries() {
        assertThat(new MaCrossoverStrategy(2, 3).generateSignals(bars(5, 5, 5, 5, 5, 5)))
                .containsOnly(Signal.HOLD);
    }

    // ----- RSI -----

    @Test
    void rsiRejectsInvalidParameters() {
        assertThatThrownBy(() -> new RsiStrategy(1, 30, 70)).hasMessageContaining("period");
        assertThatThrownBy(() -> new RsiStrategy(14, 0, 70)).hasMessageContaining("oversold");
        assertThatThrownBy(() -> new RsiStrategy(14, 30, 100)).hasMessageContaining("overbought");
        assertThatThrownBy(() -> new RsiStrategy(14, 70, 30)).hasMessageContaining("oversold");
    }

    @Test
    void rsiHoldsWhenNotEnoughBars() {
        assertThat(new RsiStrategy(5, 30, 70).generateSignals(bars(1, 2, 3, 4, 5)))
                .containsOnly(Signal.HOLD);
    }

    @Test
    void rsiBuysWhenOversoldAndSellsWhenOverbought() {
        List<Signal> falling = new RsiStrategy(3, 30, 70).generateSignals(bars(100, 90, 80, 70, 60, 50, 40));
        assertThat(falling).contains(Signal.BUY).doesNotContain(Signal.SELL);

        List<Signal> rising = new RsiStrategy(3, 30, 70).generateSignals(bars(10, 20, 30, 40, 50, 60, 70));
        assertThat(rising).contains(Signal.SELL).doesNotContain(Signal.BUY);
    }

    // ----- Factory -----

    @Test
    void factoryBuildsEachStrategyWithDefaultsOrParams() {
        assertThat(StrategyFactory.create(StrategyType.BUY_AND_HOLD, null)).isInstanceOf(BuyAndHoldStrategy.class);
        assertThat(StrategyFactory.create(StrategyType.MA_CROSSOVER, null)).isInstanceOf(MaCrossoverStrategy.class);
        assertThat(StrategyFactory.create(StrategyType.MA_CROSSOVER,
                Map.of("shortPeriod", "3", "longPeriod", 9.0))).isInstanceOf(MaCrossoverStrategy.class);
        assertThat(StrategyFactory.create(StrategyType.RSI, Map.of())).isInstanceOf(RsiStrategy.class);
        assertThat(StrategyFactory.create(StrategyType.RSI,
                Map.of("period", 7, "oversold", "25", "overbought", 75))).isInstanceOf(RsiStrategy.class);
    }

    @Test
    void factoryRejectsBadParametersAndPrediction() {
        assertThatThrownBy(() -> StrategyFactory.create(StrategyType.MA_CROSSOVER, Map.of("shortPeriod", "x")))
                .hasMessageContaining("must be an integer");
        assertThatThrownBy(() -> StrategyFactory.create(StrategyType.RSI, Map.of("oversold", "x")))
                .hasMessageContaining("must be a number");
        assertThatThrownBy(() -> StrategyFactory.create(StrategyType.PREDICTION, Map.of()))
                .hasMessageContaining("analysis service");
    }

    @Test
    void strategyTypeParsing() {
        assertThat(StrategyType.from(" rsi ")).isEqualTo(StrategyType.RSI);
        assertThatThrownBy(() -> StrategyType.from(null)).hasMessageContaining("required");
        assertThatThrownBy(() -> StrategyType.from("NOPE")).hasMessageContaining("Unknown strategyType");
    }
}
