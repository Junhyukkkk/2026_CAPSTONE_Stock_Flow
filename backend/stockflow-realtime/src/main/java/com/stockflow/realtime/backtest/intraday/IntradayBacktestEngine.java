package com.stockflow.realtime.backtest.intraday;

import com.stockflow.realtime.backtest.engine.Signal;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

/**
 * 분봉 전용 롱-온리 백테스트 엔진.
 *
 * <p>각 봉의 신호는 해당 봉이 끝난 뒤에만 알 수 있으므로, 신호가 발생한 다음 봉의 시가에서
 * 전량 매수 또는 전량 매도한다. 일봉 엔진과 시간 모델을 분리해 기존 일봉 결과에 영향을 주지 않는다.
 */
@Component
public class IntradayBacktestEngine {

    private static final MathContext MC = MathContext.DECIMAL64;
    private static final int PCT_SCALE = 6;

    public IntradayBacktestResult run(
            List<IntradayBar> bars,
            List<Signal> signals,
            BigDecimal initialCash,
            ExecutionConfig config) {
        return run(bars, signals, Signal.HOLD, initialCash, config);
    }

    /**
     * @param initialSignal 선택 구간 직전 봉 마감 때 이미 확정된 신호. Buy & Hold는 구간 첫 봉 시가에
     *                      진입하기 위해 BUY를 전달하고, 일반 기술적 전략은 HOLD 또는 직전 봉의 신호를 전달한다.
     */
    public IntradayBacktestResult run(
            List<IntradayBar> bars,
            List<Signal> signals,
            Signal initialSignal,
            BigDecimal initialCash,
            ExecutionConfig config) {
        validateInputs(bars, signals, initialSignal, initialCash, config);

        BigDecimal cash = initialCash;
        BigDecimal quantity = BigDecimal.ZERO;
        BigDecimal entryCost = null;
        BigDecimal peakEquity = initialCash;
        BigDecimal maxDrawdownPct = BigDecimal.ZERO;
        int roundTrips = 0;
        int wins = 0;
        int sequence = 0;
        List<IntradayBacktestResult.Trade> trades = new ArrayList<>();
        List<IntradayBacktestResult.EquityPoint> curve = new ArrayList<>(bars.size());

        for (int i = 0; i < bars.size(); i++) {
            IntradayBar bar = bars.get(i);
            validateBar(bar);

            // i-1 봉 마감 후 생성된 신호만 i 봉 시가에 체결한다.
            Signal executableSignal = i == 0 ? initialSignal : signals.get(i - 1);
            BigDecimal open = bar.open();
            boolean holding = quantity.signum() > 0;

            if (executableSignal == Signal.BUY && !holding && cash.signum() > 0) {
                BigDecimal executionPrice = open.multiply(BigDecimal.ONE.add(config.slippageRate()), MC);
                BigDecimal costPerUnit = executionPrice.multiply(BigDecimal.ONE.add(config.feeRate()), MC);
                entryCost = cash;
                quantity = cash.divide(costPerUnit, MC);
                cash = BigDecimal.ZERO;
                trades.add(new IntradayBacktestResult.Trade(
                        ++sequence, bar.time(), IntradayBacktestResult.Side.BUY, scale(executionPrice),
                        quantity, cash, scale(equity(cash, quantity, bar.close())), null));
            } else if (executableSignal == Signal.SELL && holding) {
                BigDecimal executionPrice = open.multiply(BigDecimal.ONE.subtract(config.slippageRate()), MC);
                BigDecimal soldQuantity = quantity;
                BigDecimal netProceeds = soldQuantity.multiply(executionPrice, MC)
                        .multiply(BigDecimal.ONE.subtract(config.feeRate()), MC);
                BigDecimal pnlPct = percentChange(entryCost, netProceeds);
                cash = cash.add(netProceeds, MC);
                quantity = BigDecimal.ZERO;
                trades.add(new IntradayBacktestResult.Trade(
                        ++sequence, bar.time(), IntradayBacktestResult.Side.SELL, scale(executionPrice),
                        soldQuantity, cash, scale(equity(cash, quantity, bar.close())), pnlPct));
                roundTrips++;
                if (pnlPct.signum() > 0) {
                    wins++;
                }
                entryCost = null;
            }

            BigDecimal equity = equity(cash, quantity, bar.close());
            if (equity.compareTo(peakEquity) > 0) {
                peakEquity = equity;
            }
            BigDecimal drawdownPct = peakEquity.signum() == 0 ? BigDecimal.ZERO
                    : peakEquity.subtract(equity).divide(peakEquity, MC)
                    .multiply(BigDecimal.valueOf(100)).setScale(PCT_SCALE, RoundingMode.HALF_UP);
            if (drawdownPct.compareTo(maxDrawdownPct) > 0) {
                maxDrawdownPct = drawdownPct;
            }
            curve.add(new IntradayBacktestResult.EquityPoint(bar.time(), scale(equity), drawdownPct));
        }

        BigDecimal finalEquity = curve.get(curve.size() - 1).equity();
        BigDecimal winRatePct = roundTrips == 0 ? null
                : BigDecimal.valueOf(wins).divide(BigDecimal.valueOf(roundTrips), MC)
                .multiply(BigDecimal.valueOf(100)).setScale(4, RoundingMode.HALF_UP);
        return new IntradayBacktestResult(
                scale(initialCash), finalEquity, percentChange(initialCash, finalEquity), maxDrawdownPct,
                roundTrips, winRatePct, bars.size(), trades, curve);
    }

    private static void validateInputs(List<IntradayBar> bars, List<Signal> signals,
                                       Signal initialSignal, BigDecimal initialCash, ExecutionConfig config) {
        if (bars == null || bars.isEmpty()) {
            throw new IllegalArgumentException("bars must not be empty");
        }
        if (signals == null || signals.size() != bars.size()) {
            throw new IllegalArgumentException("signals size must match bars size");
        }
        if (initialSignal == null) {
            throw new IllegalArgumentException("initialSignal must not be null");
        }
        if (initialCash == null || initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }
        if (config == null) {
            throw new IllegalArgumentException("execution config must not be null");
        }
    }

    private static void validateBar(IntradayBar bar) {
        if (bar == null || bar.time() == null || bar.open() == null || bar.close() == null
                || bar.open().signum() <= 0 || bar.close().signum() <= 0) {
            throw new IllegalArgumentException("bar time, open, and close must be positive");
        }
    }

    private static BigDecimal equity(BigDecimal cash, BigDecimal quantity, BigDecimal close) {
        return cash.add(quantity.multiply(close, MC), MC);
    }

    private static BigDecimal percentChange(BigDecimal from, BigDecimal to) {
        if (from == null || from.signum() == 0) {
            return BigDecimal.ZERO;
        }
        return to.subtract(from).divide(from, MC).multiply(BigDecimal.valueOf(100))
                .setScale(PCT_SCALE, RoundingMode.HALF_UP);
    }

    private static BigDecimal scale(BigDecimal value) {
        return value.setScale(8, RoundingMode.HALF_UP);
    }

    public record ExecutionConfig(BigDecimal feeRate, BigDecimal slippageRate) {
        public ExecutionConfig {
            if (feeRate == null || slippageRate == null || feeRate.signum() < 0 || slippageRate.signum() < 0
                    || feeRate.compareTo(BigDecimal.ONE) >= 0 || slippageRate.compareTo(BigDecimal.ONE) >= 0) {
                throw new IllegalArgumentException("fee and slippage rates must be in [0, 1)");
            }
        }

        public static ExecutionConfig fromBasisPoints(BigDecimal feeBps, BigDecimal slippageBps) {
            BigDecimal divisor = BigDecimal.valueOf(10000);
            return new ExecutionConfig(feeBps.divide(divisor, MC), slippageBps.divide(divisor, MC));
        }
    }
}
