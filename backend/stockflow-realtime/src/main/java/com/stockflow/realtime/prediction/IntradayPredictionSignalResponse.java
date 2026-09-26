package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** FastAPI가 반환하는 1분봉 예측 신호와 검증 지표. */
public record IntradayPredictionSignalResponse(
        String symbol,
        String model,
        @JsonProperty("from_time") Instant fromTime,
        @JsonProperty("to_time") Instant toTime,
        @JsonProperty("signal_count") int signalCount,
        @JsonProperty("buy_count") int buyCount,
        @JsonProperty("hold_count") int holdCount,
        @JsonProperty("sell_count") int sellCount,
        BigDecimal mae,
        BigDecimal rmse,
        @JsonProperty("mae_pct") BigDecimal maePct,
        @JsonProperty("rmse_pct") BigDecimal rmsePct,
        List<IntradayPredictionSignalPoint> signals
) {
    public record IntradayPredictionSignalPoint(
            @JsonProperty("signal_time") Instant signalTime,
            @JsonProperty("execution_time") Instant executionTime,
            @JsonProperty("reference_price") BigDecimal referencePrice,
            @JsonProperty("predicted_price") BigDecimal predictedPrice,
            @JsonProperty("expected_return_pct") BigDecimal expectedReturnPct,
            @JsonProperty("threshold_pct") BigDecimal thresholdPct,
            String signal
    ) {
    }
}
