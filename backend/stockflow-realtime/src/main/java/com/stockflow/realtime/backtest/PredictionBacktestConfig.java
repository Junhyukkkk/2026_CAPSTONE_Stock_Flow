package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.engine.BacktestEngine;
import com.stockflow.realtime.prediction.PredictionSignalRequest;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

record PredictionBacktestConfig(
        String model,
        String source,
        int warmup,
        int refitEvery,
        int maxHistory,
        int volatilityWindow,
        double volatilityMultiplier,
        double feeBps,
        double slippageBps
) {
    private static final Set<String> MODELS = Set.of(
            "ARIMA", "LOG_RETURN_ARIMA", "CHRONOS_BOLT");

    static PredictionBacktestConfig from(Map<String, Object> params) {
        Map<String, Object> values = params == null ? Map.of() : params;
        String model = stringParam(values, "model", "LOG_RETURN_ARIMA").toUpperCase(Locale.ROOT);
        if (!MODELS.contains(model)) {
            throw new IllegalArgumentException("model must be ARIMA, LOG_RETURN_ARIMA, or CHRONOS_BOLT");
        }

        PredictionBacktestConfig config = new PredictionBacktestConfig(
                model,
                stringParam(values, "source", "BINANCE").toUpperCase(Locale.ROOT),
                intParam(values, "warmup", 50),
                intParam(values, "refitEvery", 5),
                intParam(values, "maxHistory", 200),
                intParam(values, "volatilityWindow", 20),
                doubleParam(values, "volatilityMultiplier", 0.5),
                doubleParam(values, "feeBps", 10.0),
                doubleParam(values, "slippageBps", 5.0));
        config.validate();
        return config;
    }

    PredictionSignalRequest toRequest(String symbol, LocalDate from, LocalDate to) {
        return new PredictionSignalRequest(
                symbol.toUpperCase(Locale.ROOT), model, from, to, source,
                warmup, refitEvery, maxHistory, volatilityWindow,
                volatilityMultiplier, feeBps, slippageBps);
    }

    BacktestEngine.ExecutionConfig executionConfig() {
        return BacktestEngine.ExecutionConfig.atOpenWithCosts(
                BigDecimal.valueOf(feeBps), BigDecimal.valueOf(slippageBps));
    }

    Map<String, Object> asParams() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("model", model);
        result.put("source", source);
        result.put("warmup", warmup);
        result.put("refitEvery", refitEvery);
        result.put("maxHistory", maxHistory);
        result.put("volatilityWindow", volatilityWindow);
        result.put("volatilityMultiplier", volatilityMultiplier);
        result.put("feeBps", feeBps);
        result.put("slippageBps", slippageBps);
        return result;
    }

    private void validate() {
        if (warmup < 50 || warmup > 500) {
            throw new IllegalArgumentException("warmup must be between 50 and 500");
        }
        if (refitEvery < 1 || refitEvery > 30) {
            throw new IllegalArgumentException("refitEvery must be between 1 and 30");
        }
        if (maxHistory < warmup || maxHistory > 2000) {
            throw new IllegalArgumentException("maxHistory must be >= warmup and <= 2000");
        }
        if (volatilityWindow < 5 || volatilityWindow > 100) {
            throw new IllegalArgumentException("volatilityWindow must be between 5 and 100");
        }
        if (volatilityMultiplier < 0 || volatilityMultiplier > 5) {
            throw new IllegalArgumentException("volatilityMultiplier must be between 0 and 5");
        }
        if (feeBps < 0 || feeBps > 1000 || slippageBps < 0 || slippageBps > 1000) {
            throw new IllegalArgumentException("feeBps and slippageBps must be between 0 and 1000");
        }
    }

    private static String stringParam(Map<String, Object> params, String key, String defaultValue) {
        Object value = params.get(key);
        return value == null ? defaultValue : value.toString().trim();
    }

    private static int intParam(Map<String, Object> params, String key, int defaultValue) {
        Object value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be an integer", e);
        }
    }

    private static double doubleParam(Map<String, Object> params, String key, double defaultValue) {
        Object value = params.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(value.toString().trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(key + " must be a number", e);
        }
    }
}
