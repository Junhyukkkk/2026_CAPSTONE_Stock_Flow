package com.stockflow.realtime.backtest.engine;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/**
 * 백테스트 엔진 실행 결과(요약 지표 + 체결 내역 + 자산 곡선).
 *
 * @param winRatePct 라운드트립이 0건이면 null
 * @param cagrPct    기간/금액이 유효하지 않으면 null
 */
public record BacktestResult(
        BigDecimal initialCash,
        BigDecimal finalEquity,
        BigDecimal totalReturnPct,
        BigDecimal cagrPct,
        BigDecimal mddPct,
        int tradeCount,
        BigDecimal winRatePct,
        int barCount,
        List<Trade> trades,
        List<EquityPoint> equityCurve
) {

    /** 매수/매도 체결 한 건. */
    public record Trade(
            int seq,
            LocalDate date,
            Side side,
            BigDecimal price,
            BigDecimal quantity,
            BigDecimal cashAfter,
            BigDecimal equityAfter,
            BigDecimal pnlPct
    ) {
    }

    /** 일자별 평가금액과 낙폭. */
    public record EquityPoint(
            LocalDate date,
            BigDecimal equity,
            BigDecimal drawdownPct
    ) {
    }

    public enum Side {
        BUY,
        SELL
    }
}
