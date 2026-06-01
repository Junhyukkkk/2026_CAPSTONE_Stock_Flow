package com.stockflow.realtime.backtest.model;

/**
 * 백테스팅에서 지원하는 전략 종류.
 */
public enum StrategyType {

    /** 첫 거래일에 전량 매수 후 끝까지 보유 (벤치마크). */
    BUY_AND_HOLD,

    /** 이동평균 교차: 단기 MA가 장기 MA를 상향 돌파(골든크로스) 시 매수, 하향(데드크로스) 시 매도. */
    MA_CROSSOVER,

    /** RSI 과매도 진입 매수, 과매수 진입 매도. */
    RSI;

    /**
     * 문자열을 StrategyType 으로 파싱한다(대소문자 무시).
     *
     * @throws IllegalArgumentException 알 수 없는 전략 타입인 경우
     */
    public static StrategyType from(String value) {
        if (value == null) {
            throw new IllegalArgumentException("strategyType is required");
        }
        try {
            return StrategyType.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown strategyType: " + value + " (allowed: BUY_AND_HOLD, MA_CROSSOVER, RSI)");
        }
    }
}
