package com.stockflow.realtime.backtest.dto;

import com.stockflow.realtime.backtest.intraday.IntradayBacktestResult;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** 저장하지 않고 실행한 1분봉 백테스트 결과. */
public record IntradayBacktestResponse(
        String symbol,
        String source,
        String interval,
        String strategyType,
        Map<String, Object> params,
        Instant from,
        Instant to,
        BigDecimal initialCash,
        BigDecimal feeBps,
        BigDecimal slippageBps,
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
            String side,
            BigDecimal price,
            BigDecimal quantity,
            BigDecimal cashAfter,
            BigDecimal equityAfter,
            BigDecimal pnlPct
    ) {
        public static Trade from(IntradayBacktestResult.Trade trade) {
            return new Trade(trade.seq(), trade.time(), trade.side().name(), trade.price(), trade.quantity(),
                    trade.cashAfter(), trade.equityAfter(), trade.pnlPct());
        }
    }

    public record EquityPoint(Instant time, BigDecimal equity, BigDecimal drawdownPct) {
        public static EquityPoint from(IntradayBacktestResult.EquityPoint point) {
            return new EquityPoint(point.time(), point.equity(), point.drawdownPct());
        }
    }
}
