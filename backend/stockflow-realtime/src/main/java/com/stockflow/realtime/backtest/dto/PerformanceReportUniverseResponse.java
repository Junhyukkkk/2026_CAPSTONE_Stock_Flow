package com.stockflow.realtime.backtest.dto;

import java.time.LocalDate;
import java.util.List;

/** 전체 암호화폐 성과 리포트 실행 전 대상 종목 확인 결과. */
public record PerformanceReportUniverseResponse(
        LocalDate fromDate,
        LocalDate toDate,
        int minimumHistoryDays,
        int knownCryptoSymbolCount,
        int eligibleSymbolCount,
        List<String> eligibleSymbols
) {}
