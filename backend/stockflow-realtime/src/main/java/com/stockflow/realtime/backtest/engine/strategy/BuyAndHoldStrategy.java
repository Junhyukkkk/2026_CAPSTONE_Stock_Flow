package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.Signal;

import java.util.ArrayList;
import java.util.List;

/**
 * 첫 거래일에 매수 후 끝까지 보유하는 벤치마크 전략.
 */
public class BuyAndHoldStrategy implements TradingStrategy {

    @Override
    public List<Signal> generateSignals(List<Bar> bars) {
        List<Signal> signals = new ArrayList<>(bars.size());
        for (int i = 0; i < bars.size(); i++) {
            signals.add(i == 0 ? Signal.BUY : Signal.HOLD);
        }
        return signals;
    }
}
