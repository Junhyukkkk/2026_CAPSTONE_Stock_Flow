package com.stockflow.realtime.backtest.engine;

/**
 * 특정 일봉 시점에서 전략이 내는 매매 신호.
 * 엔진은 포지션 상태에 따라 신호를 실제 체결로 변환한다(보유 중 BUY 무시 등).
 */
public enum Signal {
    BUY,
    SELL,
    HOLD
}
