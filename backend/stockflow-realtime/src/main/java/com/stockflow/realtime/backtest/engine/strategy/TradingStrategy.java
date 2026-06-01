package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.Signal;

import java.util.List;

/**
 * 일봉 시퀀스로부터 시점별 매매 신호를 생성하는 전략.
 *
 * <p>신호 i 는 bars[0..i] 의 정보만 사용해야 한다(미래 참조 금지).
 * 반환 리스트의 크기는 입력 bars 와 동일하다.
 */
public interface TradingStrategy {

    List<Signal> generateSignals(List<Bar> bars);
}
