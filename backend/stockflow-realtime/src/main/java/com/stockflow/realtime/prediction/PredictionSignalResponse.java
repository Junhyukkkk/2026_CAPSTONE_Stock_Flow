package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

public record PredictionSignalResponse(
        String symbol,
        String model,
        @JsonProperty("from_date") LocalDate fromDate,
        @JsonProperty("to_date") LocalDate toDate,
        @JsonProperty("signal_count") int signalCount,
        @JsonProperty("buy_count") int buyCount,
        @JsonProperty("hold_count") int holdCount,
        @JsonProperty("sell_count") int sellCount,
        List<PredictionSignalPoint> signals
) {
    public record PredictionSignalPoint(
            @JsonProperty("signal_date") LocalDate signalDate,
            @JsonProperty("execution_date") LocalDate executionDate,
            @JsonProperty("reference_price") BigDecimal referencePrice,
            @JsonProperty("predicted_price") BigDecimal predictedPrice,
            @JsonProperty("expected_return_pct") BigDecimal expectedReturnPct,
            @JsonProperty("threshold_pct") BigDecimal thresholdPct,
            String signal
    ) {
    }
}
