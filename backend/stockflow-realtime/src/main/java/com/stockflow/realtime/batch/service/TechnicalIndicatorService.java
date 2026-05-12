package com.stockflow.realtime.batch.service;

import com.stockflow.realtime.batch.item.DailyIndicatorItem;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 종가 리스트(오래된 순 → 최신 순)를 받아 기술적 지표를 계산한다.
 * 데이터 부족 시 해당 지표만 null 반환 (다른 지표는 계산 가능하면 반환).
 */
@Service
public class TechnicalIndicatorService {

    private static final MathContext MC       = new MathContext(18, RoundingMode.HALF_UP);
    private static final int         SCALE    = 8;
    private static final int         RSI_PERIOD  = 14;
    private static final int         EMA_SLOW    = 26;
    private static final int         EMA_FAST    = 12;
    private static final int         EMA_SIGNAL  = 9;

    public DailyIndicatorItem compute(String symbol, LocalDate tradeDate, List<BigDecimal> closes) {
        return DailyIndicatorItem.builder()
                .symbol(symbol)
                .tradeDate(tradeDate)
                .ma5(sma(closes, 5))
                .ma20(sma(closes, 20))
                .ma60(sma(closes, 60))
                .rsi14(rsi(closes, RSI_PERIOD))
                .macd(macdLine(closes))
                .macdSignal(macdSignal(closes))
                .macdHist(macdHist(closes))
                .build();
    }

    // ── SMA ──────────────────────────────────────────────────────────────────

    public BigDecimal sma(List<BigDecimal> closes, int period) {
        if (closes.size() < period) return null;
        List<BigDecimal> window = closes.subList(closes.size() - period, closes.size());
        BigDecimal sum = window.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return sum.divide(BigDecimal.valueOf(period), SCALE, RoundingMode.HALF_UP);
    }

    // ── EMA ──────────────────────────────────────────────────────────────────

    /**
     * EMA 계산. 초기값으로 첫 'period'개 데이터의 SMA를 사용.
     */
    public BigDecimal ema(List<BigDecimal> closes, int period) {
        if (closes.size() < period) return null;

        BigDecimal multiplier = BigDecimal.valueOf(2.0 / (period + 1));
        BigDecimal oneMinusMult = BigDecimal.ONE.subtract(multiplier);

        // 초기 EMA = 첫 period개 SMA
        BigDecimal ema = sma(closes.subList(0, period), period);
        if (ema == null) return null;

        for (int i = period; i < closes.size(); i++) {
            ema = closes.get(i).multiply(multiplier, MC)
                    .add(ema.multiply(oneMinusMult, MC));
        }
        return ema.setScale(SCALE, RoundingMode.HALF_UP);
    }

    /**
     * 특정 시작 인덱스부터 EMA를 계산. MACD 시그널 계산에 사용.
     */
    private List<BigDecimal> emaList(List<BigDecimal> data, int period) {
        if (data.size() < period) return List.of();

        BigDecimal multiplier    = BigDecimal.valueOf(2.0 / (period + 1));
        BigDecimal oneMinusMult  = BigDecimal.ONE.subtract(multiplier);

        BigDecimal ema = sma(data.subList(0, period), period);
        List<BigDecimal> result = new ArrayList<>();
        result.add(ema);

        for (int i = period; i < data.size(); i++) {
            ema = data.get(i).multiply(multiplier, MC)
                    .add(ema.multiply(oneMinusMult, MC));
            result.add(ema);
        }
        return result;
    }

    // ── RSI ──────────────────────────────────────────────────────────────────

    public BigDecimal rsi(List<BigDecimal> closes, int period) {
        if (closes.size() < period + 1) return null;

        // 가격 변화량 계산
        List<BigDecimal> gains  = new ArrayList<>();
        List<BigDecimal> losses = new ArrayList<>();
        for (int i = 1; i < closes.size(); i++) {
            BigDecimal diff = closes.get(i).subtract(closes.get(i - 1));
            if (diff.compareTo(BigDecimal.ZERO) >= 0) {
                gains.add(diff);
                losses.add(BigDecimal.ZERO);
            } else {
                gains.add(BigDecimal.ZERO);
                losses.add(diff.negate());
            }
        }

        // 첫 period개의 평균 이익·손실 (Wilder's smoothed average)
        BigDecimal avgGain = sma(gains.subList(0, period), period);
        BigDecimal avgLoss = sma(losses.subList(0, period), period);
        if (avgGain == null || avgLoss == null) return null;

        BigDecimal smoothFactor = BigDecimal.ONE.divide(BigDecimal.valueOf(period), MC);
        BigDecimal oneMinusSmooth = BigDecimal.ONE.subtract(smoothFactor);

        for (int i = period; i < gains.size(); i++) {
            avgGain = gains.get(i).multiply(smoothFactor, MC)
                    .add(avgGain.multiply(oneMinusSmooth, MC));
            avgLoss = losses.get(i).multiply(smoothFactor, MC)
                    .add(avgLoss.multiply(oneMinusSmooth, MC));
        }

        if (avgLoss.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.valueOf(100).setScale(SCALE, RoundingMode.HALF_UP);
        }

        BigDecimal rs  = avgGain.divide(avgLoss, MC);
        BigDecimal rsi = BigDecimal.valueOf(100)
                .subtract(BigDecimal.valueOf(100).divide(BigDecimal.ONE.add(rs), MC));
        return rsi.setScale(4, RoundingMode.HALF_UP);
    }

    // ── MACD ─────────────────────────────────────────────────────────────────

    /** MACD 라인 (EMA12 - EMA26). 최소 26개 데이터 필요. */
    public BigDecimal macdLine(List<BigDecimal> closes) {
        BigDecimal fast = ema(closes, EMA_FAST);
        BigDecimal slow = ema(closes, EMA_SLOW);
        if (fast == null || slow == null) return null;
        return fast.subtract(slow).setScale(SCALE, RoundingMode.HALF_UP);
    }

    /** MACD 시그널 (MACD 라인의 EMA9). 최소 26+9-1=34개 데이터 필요. */
    public BigDecimal macdSignal(List<BigDecimal> closes) {
        if (closes.size() < EMA_SLOW + EMA_SIGNAL - 1) return null;

        // MACD 라인 시계열 생성
        List<BigDecimal> macdSeries = new ArrayList<>();
        for (int i = EMA_SLOW - 1; i < closes.size(); i++) {
            BigDecimal line = macdLine(closes.subList(0, i + 1));
            if (line != null) macdSeries.add(line);
        }

        return ema(macdSeries, EMA_SIGNAL);
    }

    /** MACD 히스토그램 (MACD 라인 - 시그널). */
    public BigDecimal macdHist(List<BigDecimal> closes) {
        BigDecimal line   = macdLine(closes);
        BigDecimal signal = macdSignal(closes);
        if (line == null || signal == null) return null;
        return line.subtract(signal).setScale(SCALE, RoundingMode.HALF_UP);
    }
}
