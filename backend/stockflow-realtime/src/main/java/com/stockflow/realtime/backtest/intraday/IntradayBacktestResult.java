package com.stockflow.realtime.backtest.intraday;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** 분봉 백테스트의 요약 지표, 체결 내역, 자산 곡선. */
public record IntradayBacktestResult(
        BigDecimal initialCash,
        BigDecimal finalEquity,
        BigDecimal totalReturnPct,
        BigDecimal mddPct,
        int roundTripCount,
        BigDecimal winRatePct,
        int barCount,
        List<Trade> trades,
        List<EquityPoint> equityCurve
) {

    public record Trade(
            int seq,
            Instant time,
            Side side,
            BigDecimal price,
            BigDecimal quantity,
            BigDecimal cashAfter,
            BigDecimal equityAfter,
            BigDecimal pnlPct
    ) {
    }

    public record EquityPoint(
            Instant time,
            BigDecimal equity,
            BigDecimal drawdownPct
    ) {
    }

    public enum Side {
        BUY,
        SELL
    }
}
