package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.BacktestEngine;
import com.stockflow.realtime.backtest.engine.BacktestResult;
import com.stockflow.realtime.backtest.engine.Signal;
import com.stockflow.realtime.backtest.engine.strategy.StrategyFactory;
import com.stockflow.realtime.backtest.engine.strategy.TradingStrategy;
import com.stockflow.realtime.backtest.model.StrategyType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BacktestEngineTest {

    private final BacktestEngine engine = new BacktestEngine();

    private static Bar bar(LocalDate date, double close) {
        BigDecimal c = BigDecimal.valueOf(close);
        return new Bar(date, c, c, c, c, BigDecimal.ONE);
    }

    private static List<Bar> bars(double... closes) {
        List<Bar> bars = new ArrayList<>();
        LocalDate d = LocalDate.of(2025, 1, 1);
        for (double close : closes) {
            bars.add(bar(d, close));
            d = d.plusDays(1);
        }
        return bars;
    }

    @Test
    void buyAndHold_doublesEquity_whenPriceDoubles() {
        List<Bar> bars = bars(100, 150, 200);
        TradingStrategy strategy = StrategyFactory.create(StrategyType.BUY_AND_HOLD, Map.of());

        BacktestResult result = engine.run(bars, strategy.generateSignals(bars), BigDecimal.valueOf(1000));

        // 100 -> 200, 초기 1000 => 최종 2000, 수익률 +100%
        assertThat(result.finalEquity()).isEqualByComparingTo("2000");
        assertThat(result.totalReturnPct()).isEqualByComparingTo("100");
        assertThat(result.barCount()).isEqualTo(3);
        // 끝까지 보유 → 청산(SELL) 없음 → 라운드트립 0, 승률 null
        assertThat(result.tradeCount()).isZero();
        assertThat(result.winRatePct()).isNull();
        assertThat(result.equityCurve()).hasSize(3);
    }

    @Test
    void maxDrawdown_isComputedFromPeak() {
        // 매수 후 100 -> 200(peak) -> 100 으로 하락하면 MDD 는 50%
        List<Bar> bars = bars(100, 200, 100);
        TradingStrategy strategy = StrategyFactory.create(StrategyType.BUY_AND_HOLD, Map.of());

        BacktestResult result = engine.run(bars, strategy.generateSignals(bars), BigDecimal.valueOf(1000));

        assertThat(result.mddPct()).isEqualByComparingTo("50");
        assertThat(result.totalReturnPct()).isEqualByComparingTo("0");
    }

    @Test
    void roundTrip_recordsPnlAndWinRate() {
        // bar0 BUY @100, bar1 SELL @120 -> +20% 이익 라운드트립 1건, 승률 100%
        List<Bar> bars = bars(100, 120);
        List<Signal> signals = List.of(Signal.BUY, Signal.SELL);

        BacktestResult result = engine.run(bars, signals, BigDecimal.valueOf(1000));

        assertThat(result.tradeCount()).isEqualTo(1);
        assertThat(result.winRatePct()).isEqualByComparingTo("100");
        assertThat(result.finalEquity()).isEqualByComparingTo("1200");
        assertThat(result.trades()).hasSize(2);
        assertThat(result.trades().get(1).pnlPct()).isEqualByComparingTo("20");
    }

    @Test
    void ignoresInvalidSignals_buyWhenHolding_sellWhenFlat() {
        // SELL(보유X, 무시) → BUY @100 → BUY(보유중, 무시) → 보유 유지
        List<Bar> bars = bars(100, 100, 110);
        List<Signal> signals = List.of(Signal.SELL, Signal.BUY, Signal.BUY);

        BacktestResult result = engine.run(bars, signals, BigDecimal.valueOf(1000));

        assertThat(result.trades()).hasSize(1); // BUY 한 건만 체결
        assertThat(result.trades().get(0).side()).isEqualTo(BacktestResult.Side.BUY);
        assertThat(result.finalEquity()).isEqualByComparingTo("1100");
    }

    @Test
    void rejectsNonPositiveInitialCash() {
        List<Bar> bars = bars(100, 110);
        assertThatThrownBy(() -> engine.run(bars, List.of(Signal.HOLD, Signal.HOLD), BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void maCrossover_buysOnGoldenCross() {
        // 하락 후 상승 반전: 단기(3) MA가 장기(5) MA를 상향 돌파하면 매수 발생
        List<Bar> bars = bars(100, 98, 96, 94, 92, 95, 100, 108, 118, 130);
        TradingStrategy strategy = StrategyFactory.create(
                StrategyType.MA_CROSSOVER, Map.of("shortPeriod", 3, "longPeriod", 5));

        List<Signal> signals = strategy.generateSignals(bars);

        assertThat(signals).contains(Signal.BUY);
        BacktestResult result = engine.run(bars, signals, BigDecimal.valueOf(1000));
        assertThat(result.trades()).isNotEmpty();
    }
}
