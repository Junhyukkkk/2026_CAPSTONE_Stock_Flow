package com.stockflow.realtime.backtest.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/** 변동성 기준 계수별 예측 백테스트 평균 성과. */
public record ThresholdReportResponse(
        LocalDate fromDate,
        LocalDate toDate,
        BigDecimal initialCash,
        String model,
        List<ThresholdSummary> summaries
) {
    public record ThresholdSummary(
            BigDecimal volatilityMultiplier,
            int successfulRuns,
            int failedRuns,
            int positiveReturnCount,
            BigDecimal averageTotalReturnPct,
            BigDecimal averageMddPct,
            BigDecimal averageMaePct,
            BigDecimal averageRmsePct,
            BigDecimal averageTradeCount
    ) {
    }
}
