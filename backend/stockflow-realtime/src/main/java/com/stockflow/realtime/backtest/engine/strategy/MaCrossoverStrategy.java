package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.Signal;

import java.util.ArrayList;
import java.util.List;

/**
 * 이동평균 교차(MA Crossover) 전략.
 *
 * <ul>
 *   <li>골든크로스(단기 MA가 장기 MA를 상향 돌파) → BUY</li>
 *   <li>데드크로스(단기 MA가 장기 MA를 하향 돌파) → SELL</li>
 * </ul>
 *
 * 신호 판단은 비교 연산이므로 double 로 계산한다. 실제 자산 계산은 엔진에서 BigDecimal 로 처리한다.
 */
public class MaCrossoverStrategy implements TradingStrategy {

    private final int shortPeriod;
    private final int longPeriod;

    public MaCrossoverStrategy(int shortPeriod, int longPeriod) {
        if (shortPeriod < 1 || longPeriod < 1) {
            throw new IllegalArgumentException("MA periods must be >= 1");
        }
        if (shortPeriod >= longPeriod) {
            throw new IllegalArgumentException("shortPeriod must be < longPeriod");
        }
        this.shortPeriod = shortPeriod;
        this.longPeriod = longPeriod;
    }

    @Override
    public List<Signal> generateSignals(List<Bar> bars) {
        int n = bars.size();
        List<Signal> signals = new ArrayList<>(n);
        double[] closes = new double[n];
        for (int i = 0; i < n; i++) {
            closes[i] = bars.get(i).close().doubleValue();
        }

        for (int i = 0; i < n; i++) {
            // 장기 MA + 직전 비교를 위해 (longPeriod) 개 이상의 과거 데이터 필요
            if (i < longPeriod) {
                signals.add(Signal.HOLD);
                continue;
            }
            double shortNow = sma(closes, i, shortPeriod);
            double longNow = sma(closes, i, longPeriod);
            double shortPrev = sma(closes, i - 1, shortPeriod);
            double longPrev = sma(closes, i - 1, longPeriod);

            boolean goldenCross = shortPrev <= longPrev && shortNow > longNow;
            boolean deadCross = shortPrev >= longPrev && shortNow < longNow;

            if (goldenCross) {
                signals.add(Signal.BUY);
            } else if (deadCross) {
                signals.add(Signal.SELL);
            } else {
                signals.add(Signal.HOLD);
            }
        }
        return signals;
    }

    /** endIndex 를 포함한 직전 period 개 종가의 단순 이동평균. */
    private static double sma(double[] closes, int endIndex, int period) {
        double sum = 0.0;
        for (int i = endIndex - period + 1; i <= endIndex; i++) {
            sum += closes[i];
        }
        return sum / period;
    }
}
