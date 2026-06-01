package com.stockflow.realtime.backtest.engine.strategy;

import com.stockflow.realtime.backtest.model.StrategyType;

import java.util.Map;

/**
 * StrategyType + 파라미터 Map 으로부터 {@link TradingStrategy} 를 생성한다.
 *
 * 지원 파라미터:
 * <ul>
 *   <li>MA_CROSSOVER: shortPeriod(기본 5), longPeriod(기본 20)</li>
 *   <li>RSI: period(기본 14), oversold(기본 30), overbought(기본 70)</li>
 *   <li>BUY_AND_HOLD: 없음</li>
 * </ul>
 */
public final class StrategyFactory {

    private StrategyFactory() {
    }

    public static TradingStrategy create(StrategyType type, Map<String, Object> params) {
        Map<String, Object> p = params == null ? Map.of() : params;
        return switch (type) {
            case BUY_AND_HOLD -> new BuyAndHoldStrategy();
            case MA_CROSSOVER -> new MaCrossoverStrategy(
                    intParam(p, "shortPeriod", 5),
                    intParam(p, "longPeriod", 20));
            case RSI -> new RsiStrategy(
                    intParam(p, "period", 14),
                    doubleParam(p, "oversold", 30.0),
                    doubleParam(p, "overbought", 70.0));
        };
    }

    private static int intParam(Map<String, Object> params, String key, int defaultValue) {
        Object v = params.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number num) {
            return num.intValue();
        }
        try {
            return Integer.parseInt(v.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Parameter '" + key + "' must be an integer, got: " + v);
        }
    }

    private static double doubleParam(Map<String, Object> params, String key, double defaultValue) {
        Object v = params.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Number num) {
            return num.doubleValue();
        }
        try {
            return Double.parseDouble(v.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Parameter '" + key + "' must be a number, got: " + v);
        }
    }
}
