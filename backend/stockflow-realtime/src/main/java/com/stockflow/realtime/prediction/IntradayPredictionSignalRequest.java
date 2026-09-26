package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/** FastAPI의 1분봉 walk-forward 예측 신호 요청. */
public record IntradayPredictionSignalRequest(
        String symbol,
        String model,
        @JsonProperty("from_time") Instant fromTime,
        @JsonProperty("to_time") Instant toTime,
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
