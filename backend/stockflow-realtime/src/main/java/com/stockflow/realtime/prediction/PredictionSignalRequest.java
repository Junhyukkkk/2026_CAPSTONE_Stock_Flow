package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDate;

public record PredictionSignalRequest(
        String symbol,
        String model,
        @JsonProperty("from_date") LocalDate fromDate,
        @JsonProperty("to_date") LocalDate toDate,
        String source,
        int warmup,
        @JsonProperty("refit_every") int refitEvery,
        @JsonProperty("max_history") int maxHistory,
        @JsonProperty("volatility_window") int volatilityWindow,
        @JsonProperty("volatility_multiplier") double volatilityMultiplier,
        @JsonProperty("fee_bps") double feeBps,
        @JsonProperty("slippage_bps") double slippageBps
) {
}
