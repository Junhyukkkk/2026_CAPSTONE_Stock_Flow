package com.stockflow.realtime.backtest.dto;

import java.time.Instant;

/** 1분봉 백테스트 확장 전, Timescale 연속 집계 기준으로 확인한 데이터 준비 상태. */
public record IntradayBacktestDataReadinessResponse(
        String symbol,
        String source,
        String interval,
        Instant fromTime,
        Instant toTime,
        Instant firstAvailableTime,
        Instant lastAvailableTime,
        int selectedBarCount,
        int expectedBarCount,
        int missingBarCount,
        int historyBarCount,
        int minimumHistoryBars,
        boolean canUseForBacktest,
        String status,
        String message
) {
}
