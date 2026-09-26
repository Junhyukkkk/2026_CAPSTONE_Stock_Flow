package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.engine.Signal;
import com.stockflow.realtime.backtest.intraday.IntradayBacktestEngine;
import com.stockflow.realtime.backtest.intraday.IntradayBacktestResult;
import com.stockflow.realtime.backtest.intraday.IntradayBar;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IntradayBacktestEngineTest {

    private final IntradayBacktestEngine engine = new IntradayBacktestEngine();

    @Test
    void executesCloseSignalAtNextBarOpen_withoutLookAhead() {
        List<IntradayBar> bars = List.of(
                bar("2026-09-25T08:00:00Z", 100, 110),
                bar("2026-09-25T08:01:00Z", 120, 130),
                bar("2026-09-25T08:02:00Z", 160, 150));

        IntradayBacktestResult result = engine.run(
                bars, List.of(Signal.BUY, Signal.SELL, Signal.HOLD), BigDecimal.valueOf(1000),
                IntradayBacktestEngine.ExecutionConfig.fromBasisPoints(BigDecimal.ZERO, BigDecimal.ZERO));

        assertThat(result.trades()).hasSize(2);
        assertThat(result.trades().get(0).time()).isEqualTo(Instant.parse("2026-09-25T08:01:00Z"));
        assertThat(result.trades().get(0).price()).isEqualByComparingTo("120");
        assertThat(result.trades().get(1).time()).isEqualTo(Instant.parse("2026-09-25T08:02:00Z"));
        assertThat(result.trades().get(1).price()).isEqualByComparingTo("160");
        assertThat(result.finalEquity()).isEqualByComparingTo("1333.33333333");
        assertThat(result.totalReturnPct()).isEqualByComparingTo("33.333333");
        assertThat(result.roundTripCount()).isEqualTo(1);
    }

    @Test
    void doesNotExecuteSignalFromFinalBar_withoutNextOpen() {
        IntradayBacktestResult result = engine.run(
                List.of(bar("2026-09-25T08:00:00Z", 100, 110)),
                List.of(Signal.BUY), BigDecimal.valueOf(1000),
                IntradayBacktestEngine.ExecutionConfig.fromBasisPoints(BigDecimal.ZERO, BigDecimal.ZERO));

        assertThat(result.trades()).isEmpty();
        assertThat(result.finalEquity()).isEqualByComparingTo("1000");
    }

    @Test
    void buysAtFirstOpenWhenInitialSignalWasAlreadyKnown() {
        IntradayBacktestResult result = engine.run(
                List.of(bar("2026-09-25T08:00:00Z", 100, 120)),
                List.of(Signal.HOLD), Signal.BUY, BigDecimal.valueOf(1000),
                IntradayBacktestEngine.ExecutionConfig.fromBasisPoints(BigDecimal.ZERO, BigDecimal.ZERO));

        assertThat(result.trades()).hasSize(1);
        assertThat(result.trades().get(0).price()).isEqualByComparingTo("100");
        assertThat(result.finalEquity()).isEqualByComparingTo("1200");
    }

    private static IntradayBar bar(String time, double open, double close) {
        BigDecimal high = BigDecimal.valueOf(Math.max(open, close));
        BigDecimal low = BigDecimal.valueOf(Math.min(open, close));
        return new IntradayBar(Instant.parse(time), BigDecimal.valueOf(open), high, low,
                BigDecimal.valueOf(close), BigDecimal.ONE);
    }
}
