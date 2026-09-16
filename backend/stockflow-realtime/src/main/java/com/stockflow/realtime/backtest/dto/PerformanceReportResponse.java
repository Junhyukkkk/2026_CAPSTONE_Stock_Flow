package com.stockflow.realtime.backtest.dto;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/** 동일 조건으로 실행한 종목·모델별 백테스트 성과 리포트. */
public record PerformanceReportResponse(
        LocalDate fromDate,
        LocalDate toDate,
        BigDecimal initialCash,
        List<PerformanceReportRow> rows
) {
    public record PerformanceReportRow(
            String symbol,
            String strategyType,
            String model,
            String status,
            Long runId,
            BigDecimal totalReturnPct,
            BigDecimal mddPct,
            BigDecimal mae,
            BigDecimal rmse,
            BigDecimal maePct,
            BigDecimal rmsePct,
            Integer buySignalCount,
            Integer holdSignalCount,
            Integer sellSignalCount,
            Integer tradeCount,
            String errorSummary
    ) {
    }
}
