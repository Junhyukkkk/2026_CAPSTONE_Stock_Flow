package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.Signal;

import java.util.ArrayList;
import java.util.List;

/**
 * RSI 전략. Wilder's smoothing 으로 RSI 를 계산하여
 * 과매도(oversold) 이하 진입 시 BUY, 과매수(overbought) 이상 진입 시 SELL 신호를 낸다.
 *
 * 포지션 중복 진입/청산은 엔진이 걸러내므로 여기서는 레벨 기준 신호만 생성한다.
 */
public class RsiStrategy implements TradingStrategy {

    private final int period;
    private final double oversold;
    private final double overbought;

    public RsiStrategy(int period, double oversold, double overbought) {
        if (period < 2) {
            throw new IllegalArgumentException("RSI period must be >= 2");
        }
        if (oversold <= 0 || overbought >= 100 || oversold >= overbought) {
            throw new IllegalArgumentException("require 0 < oversold < overbought < 100");
        }
        this.period = period;
        this.oversold = oversold;
        this.overbought = overbought;
    }

    @Override
    public List<Signal> generateSignals(List<Bar> bars) {
        int n = bars.size();
        List<Signal> signals = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            signals.add(Signal.HOLD);
        }
        if (n <= period) {
            return signals;
        }

        double[] closes = new double[n];
        for (int i = 0; i < n; i++) {
            closes[i] = bars.get(i).close().doubleValue();
        }

        // 초기 period 구간의 평균 상승/하락
        double avgGain = 0.0;
        double avgLoss = 0.0;
        for (int i = 1; i <= period; i++) {
            double change = closes[i] - closes[i - 1];
            if (change >= 0) {
                avgGain += change;
            } else {
                avgLoss -= change;
            }
        }
        avgGain /= period;
        avgLoss /= period;

        for (int i = period + 1; i < n; i++) {
            double change = closes[i] - closes[i - 1];
            double gain = change > 0 ? change : 0.0;
            double loss = change < 0 ? -change : 0.0;
            avgGain = (avgGain * (period - 1) + gain) / period;
            avgLoss = (avgLoss * (period - 1) + loss) / period;

            double rsi = avgLoss == 0.0 ? 100.0 : 100.0 - (100.0 / (1.0 + (avgGain / avgLoss)));

            if (rsi <= oversold) {
                signals.set(i, Signal.BUY);
            } else if (rsi >= overbought) {
                signals.set(i, Signal.SELL);
            }
        }
        return signals;
    }
}
