package com.stockflow.realtime.prediction;

import java.time.Instant;
import java.util.List;

/** 저장된 예측 실행 한 건: run 헤더 + 모델별 예측점. */
public record PredictionHistoryResponse(
        long runId,
        String symbol,
        String interval,
        int horizon,
        String source,
        Instant baseTs,
        Double baseValue,
        Instant requestedAt,
        Integer latencyMs,
        List<ModelForecast> models
) {
    public record ModelForecast(
            String model,
            Instant trainedAt,
            Double mae,
            Double rmse,
            List<Point> points
    ) {
    }

    public record Point(Instant ts, double yhat, Double yhatLower, Double yhatUpper) {
    }
}
