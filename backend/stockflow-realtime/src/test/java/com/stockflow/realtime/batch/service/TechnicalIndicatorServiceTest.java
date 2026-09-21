package com.stockflow.realtime.batch.service;

import com.stockflow.realtime.batch.item.DailyIndicatorItem;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * 지표 공식(설계 문서 "기술적 지표 계산 서비스 명세")을 손으로 검산할 수 있는
 * 작은 입력으로 고정한다. 데이터가 부족하면 해당 지표만 null 이어야 한다.
 */
class TechnicalIndicatorServiceTest {

    private final TechnicalIndicatorService service = new TechnicalIndicatorService();

    // ── SMA / EMA ─────────────────────────────────────────────────────────

    @Test
    void sma_averagesTheMostRecentPeriod() {
        List<BigDecimal> closes = closes(1, 2, 3, 4, 5, 6);
        // 최근 5개: 2,3,4,5,6 → 4
        assertThat(service.sma(closes, 5)).isEqualByComparingTo("4");
    }

    @Test
    void sma_returnsNullWhenDataIsShort() {
        assertThat(service.sma(closes(1, 2, 3), 5)).isNull();
    }

    @Test
    void ema_seedsWithSmaThenAppliesMultiplier() {
        // period 3: 가중치 2/(3+1)=0.5, 초기값 SMA(1,2,3)=2
        // EMA = 4*0.5 + 2*0.5 = 3 → 5*0.5 + 3*0.5 = 4
        assertThat(service.ema(closes(1, 2, 3, 4, 5), 3)).isEqualByComparingTo("4");
    }

    // ── RSI ───────────────────────────────────────────────────────────────

    @Test
    void rsi_isHundredWhenThereAreNoLosses() {
        BigDecimal rsi = service.rsi(closes(1, 2, 3, 4), 2);
        assertThat(rsi).isEqualByComparingTo("100");
        // 나머지 경로와 같은 scale(4) 로 반환해야 한다 (DB 컬럼 NUMERIC(8,4))
        assertThat(rsi.scale()).isEqualTo(4);
    }

    @Test
    void rsi_usesWilderSmoothing() {
        // closes 10,11,10,12 / period 2
        // 변화량 +1,-1,+2 → gains 1,0,2 / losses 0,1,0
        // 초기 avgGain=0.5, avgLoss=0.5 → 3번째: avgGain=2*0.5+0.5*0.5=1.25, avgLoss=0.25
        // RS=5, RSI=100-100/6=83.3333
        assertThat(service.rsi(closes(10, 11, 10, 12), 2)).isEqualByComparingTo("83.3333");
    }

    @Test
    void rsi_returnsNullWhenDataIsShort() {
        assertThat(service.rsi(closes(1, 2), 2)).isNull();
    }

    // ── MACD ──────────────────────────────────────────────────────────────

    @Test
    void macd_isZeroOnFlatSeries() {
        List<BigDecimal> flat = Collections.nCopies(40, new BigDecimal("100"));
        assertThat(service.macdLine(flat)).isEqualByComparingTo("0");
        assertThat(service.macdSignal(flat)).isEqualByComparingTo("0");
        assertThat(service.macdHist(flat)).isEqualByComparingTo("0");
    }

    @Test
    void macdSignal_needsAtLeast34Bars() {
        List<BigDecimal> thirtyThree = Collections.nCopies(33, new BigDecimal("100"));
        assertThat(service.macdLine(thirtyThree)).isNotNull();
        assertThat(service.macdSignal(thirtyThree)).isNull();
        assertThat(service.macdHist(thirtyThree)).isNull();
    }

    // ── 볼린저 밴드 ───────────────────────────────────────────────────────

    @Test
    void bollinger_collapsesToMeanWhenThereIsNoVariance() {
        List<BigDecimal> flat = Collections.nCopies(20, new BigDecimal("10"));
        assertThat(service.bollingerUpper(flat)).isEqualByComparingTo("10");
        assertThat(service.bollingerLower(flat)).isEqualByComparingTo("10");
    }

    @Test
    void bollinger_isMeanPlusMinusTwoStddev() {
        // 10 x19 + 30 → 평균 11, 분산 (19*1 + 361)/20 = 19, σ=√19≈4.35889894
        List<BigDecimal> closes = new ArrayList<>(Collections.nCopies(19, new BigDecimal("10")));
        closes.add(new BigDecimal("30"));

        assertThat(service.stddev(closes, 20)).isCloseTo(new BigDecimal("4.35889894"), within(new BigDecimal("0.0000001")));
        assertThat(service.bollingerUpper(closes)).isCloseTo(new BigDecimal("19.71779788"), within(new BigDecimal("0.0000001")));
        assertThat(service.bollingerLower(closes)).isCloseTo(new BigDecimal("2.28220212"), within(new BigDecimal("0.0000001")));
    }

    // ── 스토캐스틱 ─────────────────────────────────────────────────────────

    @Test
    void stochasticK_positionsCloseWithinFourteenDayRange() {
        // high 20 / low 10 고정, 마지막 종가 15 → (15-10)/(20-10)*100 = 50
        List<OhlcvData> bars = flatRangeBars(14, 20, 10, 15);
        assertThat(service.stochasticK(bars)).isEqualByComparingTo("50");
    }

    @Test
    void stochasticK_isNeutralFiftyWhenRangeIsZero() {
        List<OhlcvData> bars = flatRangeBars(14, 10, 10, 10);
        assertThat(service.stochasticK(bars)).isEqualByComparingTo("50");
    }

    @Test
    void stochasticD_isThreeDaySmaOfK() {
        // 16개 봉, high 20 / low 10 고정. 마지막 세 봉 종가 12,14,16 → %K 20,40,60 → %D 40
        List<OhlcvData> bars = new ArrayList<>(flatRangeBars(13, 20, 10, 15));
        bars.add(bar(20, 10, 12, 1));
        bars.add(bar(20, 10, 14, 1));
        bars.add(bar(20, 10, 16, 1));
        assertThat(service.stochasticD(bars)).isEqualByComparingTo("40");
    }

    @Test
    void stochastic_returnsNullWhenDataIsShort() {
        assertThat(service.stochasticK(flatRangeBars(13, 20, 10, 15))).isNull();
        assertThat(service.stochasticD(flatRangeBars(15, 20, 10, 15))).isNull();
    }

    // ── ATR ───────────────────────────────────────────────────────────────

    @Test
    void atr_isSmaOfTrueRange() {
        // bar0 h10 l8 c9 / bar1 h12 l9 c11 → TR=max(3,|12-9|,|9-9|)=3
        // bar2 h13 l10 c12 → TR=max(3,|13-11|,|10-11|)=3 → ATR(2)=3
        List<OhlcvData> bars = List.of(bar(10, 8, 9, 1), bar(12, 9, 11, 1), bar(13, 10, 12, 1));
        assertThat(service.atr(bars, 2)).isEqualByComparingTo("3");
    }

    @Test
    void atr_needsPeriodPlusOneBars() {
        assertThat(service.atr(List.of(bar(10, 8, 9, 1), bar(12, 9, 11, 1)), 2)).isNull();
    }

    // ── OBV ───────────────────────────────────────────────────────────────

    @Test
    void obv_addsVolumeOnUpAndSubtractsOnDown() {
        // close 10→11 (+200), 11→11 (0), 11→9 (-400) → -200
        List<OhlcvData> bars = List.of(bar(10, 10, 10, 100), bar(11, 11, 11, 200), bar(11, 11, 11, 300), bar(9, 9, 9, 400));
        assertThat(service.obv(bars)).isEqualTo(-200L);
    }

    @Test
    void obv_isNullOnEmptyInput() {
        assertThat(service.obv(List.of())).isNull();
    }

    // ── 통합 진입점 ────────────────────────────────────────────────────────

    @Test
    void computeWithOhlcv_fillsOnlyIndicatorsThatHaveEnoughData() {
        List<OhlcvData> fiveBars = IntStream.rangeClosed(1, 5)
                .mapToObj(i -> bar(i + 1, i - 1, i, 10))
                .toList();

        DailyIndicatorItem item = service.computeWithOhlcv("TEST", LocalDate.of(2026, 9, 21), fiveBars);

        assertThat(item.getSymbol()).isEqualTo("TEST");
        assertThat(item.getTradeDate()).isEqualTo(LocalDate.of(2026, 9, 21));
        assertThat(item.getMa5()).isEqualByComparingTo("3");
        assertThat(item.getMa20()).isNull();
        assertThat(item.getMa60()).isNull();
        assertThat(item.getRsi14()).isNull();
        assertThat(item.getMacd()).isNull();
        assertThat(item.getBbUpper()).isNull();
        assertThat(item.getStochK()).isNull();
        assertThat(item.getAtr14()).isNull();
        assertThat(item.getObv()).isEqualTo(40L); // 매일 상승 → 10*4
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static List<BigDecimal> closes(int... values) {
        return IntStream.of(values).mapToObj(BigDecimal::valueOf).toList();
    }

    private static OhlcvData bar(int high, int low, int close, int volume) {
        return new OhlcvData(BigDecimal.valueOf(close), BigDecimal.valueOf(high),
                BigDecimal.valueOf(low), BigDecimal.valueOf(close), BigDecimal.valueOf(volume));
    }

    /** high/low 를 고정하고 종가만 lastClose 인 봉 n개 (모든 봉 종가 동일). */
    private static List<OhlcvData> flatRangeBars(int n, int high, int low, int lastClose) {
        return Collections.nCopies(n, bar(high, low, lastClose, 1));
    }
}
