package com.stockflow.realtime.backtest.dto;

import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportSummary;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/** DB에 저장되는 전체 성과 리포트 작업의 진행 상태와 결과. */
public record PerformanceReportJobResponse(
        long id,
        String status,
        LocalDate fromDate,
        LocalDate toDate,
        BigDecimal initialCash,
        int minimumHistoryDays,
        int totalSymbols,
        int completedSymbols,
        int successfulRows,
        int failedRows,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        String errorSummary,
        List<PerformanceReportRow> rows,
        List<PerformanceReportSummary> summaries
) {
}
