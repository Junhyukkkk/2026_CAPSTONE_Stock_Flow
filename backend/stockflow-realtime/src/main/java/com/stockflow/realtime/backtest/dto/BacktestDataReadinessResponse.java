package com.stockflow.realtime.backtest.dto;

import java.time.LocalDate;

/**
 * 백테스트 실행 전에 화면에서 보여 주는 일봉 데이터 준비 상태.
 * 실제 저장된 관측치 수를 기준으로 판단하므로 달력상 날짜와 구분된다.
 */
public record BacktestDataReadinessResponse(
        String symbol,
        String source,
        LocalDate fromDate,
        LocalDate toDate,
        LocalDate firstAvailableDate,
        LocalDate lastAvailableDate,
        int selectedBarCount,
        int expectedBarCount,
        int missingBarCount,
        int historyBarCount,
        int minimumHistoryDays,
        boolean canRun,
        String status,
        String message
) {
}
