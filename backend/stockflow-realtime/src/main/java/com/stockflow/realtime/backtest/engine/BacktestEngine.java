package com.stockflow.realtime.backtest.engine;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * 단순 롱-온리(long-only) 백테스트 엔진.
 *
 * <p>매매 규칙
 * <ul>
 *   <li>현금 보유 중 BUY 신호 → 종가에 전량 매수(분할 수량 허용)</li>
 *   <li>주식 보유 중 SELL 신호 → 종가에 전량 매도</li>
 *   <li>보유 중 BUY, 현금 중 SELL 등 무효 신호는 무시</li>
 * </ul>
 *
 * <p>자산 평가는 매 봉의 종가 기준으로 계산하며, 자산 곡선/낙폭을 함께 산출한다.
 */
@Component
public class BacktestEngine {

    private static final MathContext MC = MathContext.DECIMAL64;
    private static final int PCT_SCALE = 6;

    /**
     * @param bars      오래된 순 → 최신 순으로 정렬된 일봉
     * @param signals   bars 와 같은 크기의 시점별 신호
     * @param initialCash 초기 자본
     */
    public BacktestResult run(List<Bar> bars, List<Signal> signals, BigDecimal initialCash) {
        if (bars == null || bars.isEmpty()) {
            throw new IllegalArgumentException("bars must not be empty");
        }
        if (signals == null || signals.size() != bars.size()) {
            throw new IllegalArgumentException("signals size must match bars size");
        }
        if (initialCash == null || initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }

        BigDecimal cash = initialCash;
        BigDecimal quantity = BigDecimal.ZERO;
        BigDecimal entryPrice = null; // 현재 보유 포지션의 매수가

        List<BacktestResult.Trade> trades = new ArrayList<>();
        List<BacktestResult.EquityPoint> curve = new ArrayList<>(bars.size());

        BigDecimal peakEquity = initialCash;
        BigDecimal maxDrawdownPct = BigDecimal.ZERO;
        int roundTrips = 0;
        int wins = 0;
        int seq = 0;

        for (int i = 0; i < bars.size(); i++) {
            Bar bar = bars.get(i);
            BigDecimal close = bar.close();
            Signal signal = signals.get(i);

            boolean holding = quantity.signum() > 0;

            if (signal == Signal.BUY && !holding && cash.signum() > 0) {
                quantity = cash.divide(close, MC);
                entryPrice = close;
                cash = BigDecimal.ZERO;
                BigDecimal equity = equity(cash, quantity, close);
                trades.add(new BacktestResult.Trade(
                        ++seq, bar.date(), BacktestResult.Side.BUY,
                        close, quantity, cash, equity, null));
            } else if (signal == Signal.SELL && holding) {
                BigDecimal proceeds = quantity.multiply(close, MC);
                BigDecimal pnlPct = percentChange(entryPrice, close);
                cash = cash.add(proceeds, MC);
                quantity = BigDecimal.ZERO;
                BigDecimal equity = equity(cash, quantity, close);
                trades.add(new BacktestResult.Trade(
                        ++seq, bar.date(), BacktestResult.Side.SELL,
                        close, proceeds.divide(close, MC), cash, equity, pnlPct));
                roundTrips++;
                if (pnlPct.signum() > 0) {
                    wins++;
                }
                entryPrice = null;
            }

            BigDecimal equity = equity(cash, quantity, close);
            if (equity.compareTo(peakEquity) > 0) {
                peakEquity = equity;
            }
            BigDecimal drawdownPct = peakEquity.signum() == 0
                    ? BigDecimal.ZERO
                    : peakEquity.subtract(equity).divide(peakEquity, MC)
                    .multiply(BigDecimal.valueOf(100)).setScale(PCT_SCALE, RoundingMode.HALF_UP);
            if (drawdownPct.compareTo(maxDrawdownPct) > 0) {
                maxDrawdownPct = drawdownPct;
            }
            curve.add(new BacktestResult.EquityPoint(bar.date(), scale(equity), drawdownPct));
        }

        BigDecimal finalEquity = curve.get(curve.size() - 1).equity();
        BigDecimal totalReturnPct = percentChange(initialCash, finalEquity);
        BigDecimal cagrPct = cagr(initialCash, finalEquity,
                bars.get(0).date(), bars.get(bars.size() - 1).date());
        BigDecimal winRatePct = roundTrips == 0
                ? null
                : BigDecimal.valueOf(wins)
                .divide(BigDecimal.valueOf(roundTrips), MC)
                .multiply(BigDecimal.valueOf(100))
                .setScale(4, RoundingMode.HALF_UP);

        return new BacktestResult(
                scale(initialCash), finalEquity, totalReturnPct, cagrPct, maxDrawdownPct,
                roundTrips, winRatePct, bars.size(), trades, curve);
    }

    private static BigDecimal equity(BigDecimal cash, BigDecimal quantity, BigDecimal close) {
        return scale(cash.add(quantity.multiply(close, MC), MC));
    }

    private static BigDecimal percentChange(BigDecimal from, BigDecimal to) {
        if (from == null || from.signum() == 0) {
            return BigDecimal.ZERO;
        }
        return to.subtract(from)
                .divide(from, MC)
                .multiply(BigDecimal.valueOf(100))
                .setScale(PCT_SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal cagr(BigDecimal initial, BigDecimal finalEquity,
                                   java.time.LocalDate start, java.time.LocalDate end) {
        if (initial == null || initial.signum() <= 0 || finalEquity == null || finalEquity.signum() <= 0) {
            return null;
        }
        long days = ChronoUnit.DAYS.between(start, end);
        if (days <= 0) {
            return null;
        }
        double years = days / 365.25;
        double ratio = finalEquity.doubleValue() / initial.doubleValue();
        double cagr = Math.pow(ratio, 1.0 / years) - 1.0;
        if (Double.isNaN(cagr) || Double.isInfinite(cagr)) {
            return null;
        }
        return BigDecimal.valueOf(cagr * 100.0).setScale(PCT_SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal scale(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }
}
