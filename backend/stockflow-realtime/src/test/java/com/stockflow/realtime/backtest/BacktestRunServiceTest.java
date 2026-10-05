package com.stockflow.realtime.backtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stockflow.realtime.backtest.BacktestRunService.NoDataException;
import com.stockflow.realtime.backtest.dto.BacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestResponse;
import com.stockflow.realtime.backtest.dto.IntradayRunRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.PerformanceReportUniverseResponse;
import com.stockflow.realtime.backtest.dto.RunRequest;
import com.stockflow.realtime.backtest.dto.ThresholdReportRequest;
import com.stockflow.realtime.backtest.dto.ThresholdReportResponse;
import com.stockflow.realtime.backtest.engine.BacktestEngine;
import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.intraday.IntradayBacktestEngine;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.DataCoverage;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.EquityRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.PredictionPointRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.RunRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.TradeRow;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import com.stockflow.realtime.prediction.IntradayPredictionSignalResponse;
import com.stockflow.realtime.prediction.IntradayPredictionSignalResponse.IntradayPredictionSignalPoint;
import com.stockflow.realtime.prediction.PredictionService;
import com.stockflow.realtime.prediction.PredictionSignalResponse;
import com.stockflow.realtime.prediction.PredictionSignalResponse.PredictionSignalPoint;
import com.stockflow.realtime.stock.IntradayOhlcvService;
import com.stockflow.realtime.stock.dto.IntradayOhlcvResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class BacktestRunServiceTest {

    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final LocalDate FROM = LocalDate.of(2025, 1, 1);
    private static final LocalDate TO = LocalDate.of(2025, 1, 10);
    private static final Instant T0 = Instant.parse("2025-01-01T00:00:00Z");

    @Mock BacktestStrategyRepository strategyRepository;
    @Mock BacktestRunRepository runRepository;
    @Mock PredictionService predictionService;
    @Mock IntradayOhlcvService intradayOhlcvService;

    BacktestRunService service;

    @BeforeEach
    void setUp() {
        service = new BacktestRunService(strategyRepository, runRepository, new BacktestEngine(),
                predictionService, intradayOhlcvService, new IntradayBacktestEngine());
    }

    // ---------- helpers ----------

    private static <T> T json(Class<T> type, String body) {
        try {
            return JSON.readValue(body, type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Bar> dailyBars(int count) {
        List<Bar> bars = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            BigDecimal price = BigDecimal.valueOf(100 + i);
            bars.add(new Bar(FROM.plusDays(i), price, price, price, price, BigDecimal.TEN));
        }
        return bars;
    }

    private static List<IntradayOhlcvResponse> minuteBars(Instant start, int count) {
        List<IntradayOhlcvResponse> bars = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            BigDecimal price = BigDecimal.valueOf(100 + i);
            bars.add(IntradayOhlcvResponse.builder()
                    .symbol("BTCUSDT").time(start.plusSeconds(60L * i))
                    .open(price).high(price).low(price).close(price)
                    .volume(BigDecimal.ONE).tickCount(1L).build());
        }
        return bars;
    }

    private static RunRow runRow(long id) {
        return new RunRow(id, null, "BTCUSDT", "BUY_AND_HOLD", Map.of("mae", "1.5", "buySignalCount", 3),
                FROM, TO, BigDecimal.valueOf(10000), BigDecimal.valueOf(11000), BigDecimal.TEN,
                BigDecimal.ONE, BigDecimal.valueOf(2), 1, BigDecimal.valueOf(50), 10, "SUCCESS", Instant.EPOCH);
    }

    private void stubSuccessfulSave(long runId) {
        when(runRepository.saveResult(any(), anyString(), anyString(), anyMap(), any(), any(), any()))
                .thenReturn(runId);
        when(runRepository.findRun(runId)).thenReturn(Optional.of(runRow(runId)));
    }

    // ---------- saved / ad-hoc runs ----------

    @Test
    void runSavedStrategy_returnsEmptyWhenStrategyMissing() {
        when(strategyRepository.findById(1L)).thenReturn(Optional.empty());

        assertThat(service.runSavedStrategy(1L, FROM, TO)).isEmpty();
    }

    @Test
    void runSavedStrategy_executesStoredStrategy() {
        when(strategyRepository.findById(7L)).thenReturn(Optional.of(new StrategyRow(
                7L, "s", "BTCUSDT", "BUY_AND_HOLD", Map.of(), BigDecimal.valueOf(5000), Instant.EPOCH, Instant.EPOCH)));
        when(runRepository.loadBars("BTCUSDT", "BINANCE", FROM, TO)).thenReturn(dailyBars(10));
        stubSuccessfulSave(99L);

        Optional<BacktestRunResponse> response = service.runSavedStrategy(7L, FROM, TO);

        assertThat(response).isPresent();
        assertThat(response.get().getId()).isEqualTo(99L);
        assertThat(response.get().getStatus()).isEqualTo("SUCCESS");
        verify(runRepository).savePredictionPoints(eq(99L), anyList());
    }

    @Test
    void runAdHoc_usesDefaultInitialCashAndRunsStrategy() {
        when(runRepository.loadBars("BTCUSDT", "BINANCE", FROM, TO)).thenReturn(dailyBars(10));
        stubSuccessfulSave(5L);
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"buy_and_hold\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        BacktestRunResponse response = service.runAdHoc(request);

        assertThat(response.getId()).isEqualTo(5L);
        verify(runRepository).saveResult(eq(null), eq("BTCUSDT"), eq("BUY_AND_HOLD"), anyMap(),
                eq(FROM), eq(TO), org.mockito.ArgumentMatchers.argThat(r ->
                        r.initialCash().compareTo(BigDecimal.valueOf(10000)) == 0));
    }

    @Test
    void runAdHoc_rejectsInvalidDateRangeAndCash() {
        RunRequest reversed = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\",\"from\":\"2025-02-01\",\"to\":\"2025-01-01\"}");
        assertThatThrownBy(() -> service.runAdHoc(reversed))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("from must be <= to");

        RunRequest negativeCash = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\",\"initialCash\":-1,"
                        + "\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");
        assertThatThrownBy(() -> service.runAdHoc(negativeCash))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("initialCash must be positive");
        verify(runRepository, never()).saveFailure(any(), anyString(), anyString(), anyMap(), any(), any(), any(), any());
    }

    @Test
    void runAdHoc_throwsNoDataWithoutRecordingFailure() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(List.of());
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"MA_CROSSOVER\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        assertThatThrownBy(() -> service.runAdHoc(request)).isInstanceOf(NoDataException.class);
        verify(runRepository, never()).saveFailure(any(), anyString(), anyString(), anyMap(), any(), any(), any(), any());
    }

    @Test
    void runAdHoc_recordsFailureAndRethrowsUnexpectedErrors() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(dailyBars(10));
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"MA_CROSSOVER\","
                        + "\"params\":{\"shortPeriod\":\"abc\"},\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        assertThatThrownBy(() -> service.runAdHoc(request)).isInstanceOf(IllegalArgumentException.class);
        verify(runRepository).saveFailure(eq(null), eq("BTCUSDT"), eq("MA_CROSSOVER"), anyMap(),
                eq(FROM), eq(TO), any(BigDecimal.class), anyString());
    }

    @Test
    void runAdHoc_predictionRequiresEnoughHistory() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(dailyBars(10));
        when(runRepository.countBarsBefore("BTCUSDT", "BINANCE", FROM)).thenReturn(10);
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"PREDICTION\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        assertThatThrownBy(() -> service.runAdHoc(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 50 observations");
        verify(runRepository).saveFailure(any(), anyString(), anyString(), anyMap(), any(), any(), any(), anyString());
    }

    @Test
    void runAdHoc_predictionAlignsSignalsByExecutionDate() {
        List<Bar> bars = dailyBars(3);
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(bars);
        when(runRepository.countBarsBefore(anyString(), anyString(), any())).thenReturn(80);
        List<PredictionSignalPoint> points = List.of(
                new PredictionSignalPoint(FROM.minusDays(1), FROM, BigDecimal.ONE, BigDecimal.valueOf(2),
                        BigDecimal.ONE, BigDecimal.ONE, "buy"),
                new PredictionSignalPoint(FROM, FROM.plusDays(1), BigDecimal.ONE, BigDecimal.valueOf(2),
                        BigDecimal.ONE, BigDecimal.ONE, "HOLD"),
                new PredictionSignalPoint(FROM.plusDays(1), FROM.plusDays(2), BigDecimal.ONE, BigDecimal.valueOf(2),
                        BigDecimal.ONE, BigDecimal.ONE, "SELL"));
        when(predictionService.backtestSignals(any())).thenReturn(new PredictionSignalResponse(
                "BTCUSDT", "LOG_RETURN_ARIMA", FROM, TO, 3, 1, 1, 1,
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, points));
        stubSuccessfulSave(11L);
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"btcusdt\",\"strategyType\":\"PREDICTION\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        BacktestRunResponse response = service.runAdHoc(request);

        assertThat(response.getId()).isEqualTo(11L);
        verify(runRepository).savePredictionPoints(11L, points);
        verify(runRepository).saveResult(any(), anyString(), eq("PREDICTION"),
                org.mockito.ArgumentMatchers.argThat(p -> p.containsKey("buySignalCount")
                        && p.containsKey("maePct")),
                any(), any(), any());
    }

    @Test
    void runAdHoc_predictionFailsOnMissingDuplicateOrNullSignals() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(dailyBars(2));
        when(runRepository.countBarsBefore(anyString(), anyString(), any())).thenReturn(80);
        RunRequest request = json(RunRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"PREDICTION\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");

        PredictionSignalPoint only = new PredictionSignalPoint(FROM, FROM, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.ONE, BigDecimal.ONE, "BUY");
        when(predictionService.backtestSignals(any())).thenReturn(new PredictionSignalResponse(
                "BTCUSDT", "M", FROM, TO, 1, 1, 0, 0, null, null, null, null, List.of(only)));
        assertThatThrownBy(() -> service.runAdHoc(request))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Missing prediction signal");

        when(predictionService.backtestSignals(any())).thenReturn(new PredictionSignalResponse(
                "BTCUSDT", "M", FROM, TO, 2, 2, 0, 0, null, null, null, null, List.of(only, only)));
        assertThatThrownBy(() -> service.runAdHoc(request))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("Duplicate prediction signal date");

        when(predictionService.backtestSignals(any())).thenReturn(new PredictionSignalResponse(
                "BTCUSDT", "M", FROM, TO, 0, 0, 0, 0, null, null, null, null, null));
        assertThatThrownBy(() -> service.runAdHoc(request))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("no signals");
    }

    // ---------- daily readiness ----------

    @Test
    void inspectDataReadiness_readyWhenComplete() {
        when(runRepository.loadBars("BTCUSDT", "BINANCE", FROM, TO)).thenReturn(dailyBars(10));
        when(runRepository.countBarsBefore("BTCUSDT", "BINANCE", FROM)).thenReturn(60);
        when(runRepository.findCoverage("BTCUSDT", "BINANCE"))
                .thenReturn(new DataCoverage(LocalDate.of(2020, 1, 1), TO, 2000));

        BacktestDataReadinessResponse r = service.inspectDataReadiness(" btcusdt ", FROM, TO, null, 50);

        assertThat(r.canRun()).isTrue();
        assertThat(r.status()).isEqualTo("READY");
        assertThat(r.symbol()).isEqualTo("BTCUSDT");
        assertThat(r.expectedBarCount()).isEqualTo(10);
        assertThat(r.missingBarCount()).isZero();
    }

    @Test
    void inspectDataReadiness_blockedCases() {
        when(runRepository.findCoverage(anyString(), anyString())).thenReturn(new DataCoverage(null, null, 0));
        when(runRepository.countBarsBefore(anyString(), anyString(), any())).thenReturn(10);

        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(List.of());
        assertThat(service.inspectDataReadiness("BTCUSDT", FROM, TO, "binance", 0).message())
                .contains("일봉 데이터가 없습니다");

        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(dailyBars(10));
        BacktestDataReadinessResponse noHistory = service.inspectDataReadiness("BTCUSDT", FROM, TO, "BINANCE", 50);
        assertThat(noHistory.canRun()).isFalse();
        assertThat(noHistory.message()).contains("학습 데이터가 부족");

        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(dailyBars(5));
        BacktestDataReadinessResponse gaps = service.inspectDataReadiness("BTCUSDT", FROM, TO, "BINANCE", 0);
        assertThat(gaps.missingBarCount()).isEqualTo(5);
        assertThat(gaps.message()).contains("누락된 일봉");
    }

    @Test
    void inspectDataReadiness_validatesInput() {
        assertThatThrownBy(() -> service.inspectDataReadiness("BTCUSDT", FROM, TO, null, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.inspectDataReadiness("  ", FROM, TO, null, 0))
                .hasMessageContaining("symbol is required");
        assertThatThrownBy(() -> service.inspectDataReadiness(null, FROM, TO, null, 0))
                .hasMessageContaining("symbol is required");
        assertThatThrownBy(() -> service.inspectDataReadiness("BTCUSDT", null, TO, null, 0))
                .hasMessageContaining("required");
    }

    // ---------- intraday readiness ----------

    @Test
    void inspectIntradayDataReadiness_readyWhenContinuous() {
        Instant end = T0.plusSeconds(60 * 60);
        when(intradayOhlcvService.getIntraday(eq("BTCUSDT"), eq("1m"), eq(T0), eq(end)))
                .thenReturn(minuteBars(T0, 60));
        when(intradayOhlcvService.getIntraday(eq("BTCUSDT"), eq("1m"), eq(T0.minusSeconds(60L * 100)), eq(T0)))
                .thenReturn(minuteBars(T0.minusSeconds(60L * 60), 60));

        IntradayBacktestDataReadinessResponse r =
                service.inspectIntradayDataReadiness("btcusdt", T0, end, "1m", "binance", 50);

        assertThat(r.canUseForBacktest()).isTrue();
        assertThat(r.status()).isEqualTo("READY");
        assertThat(r.selectedBarCount()).isEqualTo(60);
        assertThat(r.firstAvailableTime()).isEqualTo(T0.minusSeconds(3600));
    }

    @Test
    void inspectIntradayDataReadiness_blockedMessages() {
        Instant end = T0.plusSeconds(600);
        when(intradayOhlcvService.getIntraday(anyString(), anyString(), any(), any())).thenReturn(List.of());
        IntradayBacktestDataReadinessResponse empty =
                service.inspectIntradayDataReadiness("BTCUSDT", T0, end, "1m", null, 50);
        assertThat(empty.message()).contains("사용할 수 있는 1분봉 데이터가 없습니다");
        assertThat(empty.firstAvailableTime()).isNull();
        assertThat(empty.lastAvailableTime()).isNull();

        when(intradayOhlcvService.getIntraday(anyString(), anyString(), eq(T0), eq(end)))
                .thenReturn(minuteBars(T0, 5));
        assertThat(service.inspectIntradayDataReadiness("BTCUSDT", T0, end, "1m", null, 50).message())
                .contains("누락된 1분봉");

        when(intradayOhlcvService.getIntraday(anyString(), anyString(), eq(T0), eq(end)))
                .thenReturn(minuteBars(T0, 10));
        IntradayBacktestDataReadinessResponse noHistory =
                service.inspectIntradayDataReadiness("BTCUSDT", T0, end, "1m", null, 50);
        assertThat(noHistory.message()).contains("학습 데이터가 부족");
        assertThat(noHistory.firstAvailableTime()).isEqualTo(T0);
    }

    @Test
    void inspectIntradayDataReadiness_validatesInput() {
        Instant end = T0.plusSeconds(600);
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness("BTCUSDT", T0, end, "5m", null, 50))
                .hasMessageContaining("only 1m");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness("BTCUSDT", null, end, "1m", null, 50))
                .hasMessageContaining("earlier");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness("BTCUSDT", end, T0, "1m", null, 50))
                .hasMessageContaining("earlier");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness(
                "BTCUSDT", T0, T0.plusSeconds(7 * 3600), "1m", null, 50)).hasMessageContaining("6 hours");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness("BTCUSDT", T0, end, "1m", null, 10))
                .hasMessageContaining("between 50 and 500");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness(
                "BTCUSDT", T0, T0.plusSeconds(30), "1m", null, 50)).hasMessageContaining("one-minute bar");
        assertThatThrownBy(() -> service.inspectIntradayDataReadiness(" ", T0, end, "1m", null, 50))
                .hasMessageContaining("symbol is required");
    }

    // ---------- intraday run ----------

    private IntradayRunRequest intraday(String body) {
        return json(IntradayRunRequest.class, body);
    }

    @Test
    void runIntraday_buyAndHold() {
        Instant end = T0.plusSeconds(600);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 10));

        IntradayBacktestResponse response = service.runIntraday(intraday(
                "{\"symbol\":\"btcusdt\",\"strategyType\":\"BUY_AND_HOLD\",\"from\":\"2025-01-01T00:00:00Z\","
                        + "\"to\":\"2025-01-01T00:10:00Z\",\"feeBps\":0,\"slippageBps\":0}"));

        assertThat(response.strategyType()).isEqualTo("BUY_AND_HOLD");
        assertThat(response.barCount()).isEqualTo(10);
        assertThat(response.trades()).isNotEmpty();
        assertThat(response.equityCurve()).hasSize(10);
        assertThat(response.predictionPoints()).isEmpty();
        assertThat(response.feeBps()).isEqualByComparingTo("0");
    }

    @Test
    void runIntraday_maCrossoverNeedsHistory() {
        Instant end = T0.plusSeconds(600);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 10));
        String body = "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"MA_CROSSOVER\","
                + "\"params\":{\"shortPeriod\":2,\"longPeriod\":4},"
                + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:10:00Z\"}";

        when(intradayOhlcvService.getIntraday(eq("BTCUSDT"), eq("1m"), eq(T0.minusSeconds(300)), eq(T0)))
                .thenReturn(minuteBars(T0.minusSeconds(300), 5));
        IntradayBacktestResponse ok = service.runIntraday(intraday(body));
        assertThat(ok.params()).containsEntry("shortPeriod", 2).containsEntry("longPeriod", 4);
        assertThat(ok.barCount()).isEqualTo(10);

        when(intradayOhlcvService.getIntraday(eq("BTCUSDT"), eq("1m"), eq(T0.minusSeconds(300)), eq(T0)))
                .thenReturn(minuteBars(T0.minusSeconds(120), 2));
        assertThatThrownBy(() -> service.runIntraday(intraday(body)))
                .isInstanceOf(NoDataException.class).hasMessageContaining("5개");
    }

    @Test
    void runIntraday_maCrossoverRejectsBadPeriods() {
        Instant end = T0.plusSeconds(600);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 10));
        String template = "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"MA_CROSSOVER\",\"params\":%s,"
                + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:10:00Z\"}";

        assertThatThrownBy(() -> service.runIntraday(intraday(
                template.formatted("{\"shortPeriod\":10,\"longPeriod\":5}"))))
                .hasMessageContaining("1 <= shortPeriod < longPeriod");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                template.formatted("{\"shortPeriod\":0}"))))
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                template.formatted("{\"shortPeriod\":\"x\"}"))))
                .hasMessageContaining("must be an integer");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                template.formatted("{\"feeBps\":\"abc\"}"))))
                .hasMessageContaining("must be a number");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                template.formatted("{\"feeBps\":5000}"))))
                .hasMessageContaining("between 0 and 1000");
    }

    @Test
    void runIntraday_validatesRequest() {
        String base = "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:10:00Z\"";
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"ETHUSDT\",\"strategyType\":\"BUY_AND_HOLD\"," + base + "}")))
                .hasMessageContaining("BTCUSDT only");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\"," + base + "}")))
                .hasMessageContaining("supports BUY_AND_HOLD");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"BUY_AND_HOLD\",\"from\":\"2025-01-01T00:00:00Z\","
                        + "\"to\":\"2025-01-01T08:00:00Z\"}")))
                .hasMessageContaining("within 6 hours");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"BUY_AND_HOLD\",\"initialCash\":0," + base + "}")))
                .hasMessageContaining("initialCash must be positive");
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"BUY_AND_HOLD\",\"from\":\"2025-01-01T00:00:00Z\"}")))
                .hasMessageContaining("required");
        when(intradayOhlcvService.getIntraday(anyString(), anyString(), any(), any())).thenReturn(minuteBars(T0, 3));
        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"BUY_AND_HOLD\"," + base + "}")))
                .isInstanceOf(NoDataException.class);
    }

    private IntradayPredictionSignalResponse intradayPrediction(List<IntradayPredictionSignalPoint> points) {
        return new IntradayPredictionSignalResponse("BTCUSDT", "LOG_RETURN_ARIMA", T0, T0.plusSeconds(180),
                points == null ? 0 : points.size(), 1, 1, 1,
                BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ONE, points);
    }

    private static IntradayPredictionSignalPoint point(int minute, String signal) {
        return new IntradayPredictionSignalPoint(T0.plusSeconds(60L * (minute - 1)), T0.plusSeconds(60L * minute),
                BigDecimal.ONE, BigDecimal.valueOf(2), BigDecimal.ONE, BigDecimal.ONE, signal);
    }

    @Test
    void runIntraday_predictionAlignsSignals() {
        Instant end = T0.plusSeconds(180);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 3));
        when(predictionService.backtestIntradaySignals(any())).thenReturn(
                intradayPrediction(List.of(point(0, "BUY"), point(1, "hold"), point(2, "SELL"))));

        IntradayBacktestResponse response = service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"PREDICTION\","
                        + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:03:00Z\"}"));

        assertThat(response.strategyType()).isEqualTo("PREDICTION");
        assertThat(response.predictionPoints()).hasSize(3);
        assertThat(response.params()).containsKeys("buySignalCount", "rmsePct", "model");
        assertThat(response.feeBps()).isEqualByComparingTo("10");
    }

    @Test
    void runIntraday_predictionRejectsBadResponses() {
        Instant end = T0.plusSeconds(180);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 3));
        String body = "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"PREDICTION\","
                + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:03:00Z\"}";

        when(predictionService.backtestIntradaySignals(any())).thenReturn(intradayPrediction(null));
        assertThatThrownBy(() -> service.runIntraday(intraday(body))).hasMessageContaining("no intraday signals");

        when(predictionService.backtestIntradaySignals(any())).thenReturn(
                intradayPrediction(List.of(point(0, "BOGUS"))));
        assertThatThrownBy(() -> service.runIntraday(intraday(body))).hasMessageContaining("Invalid intraday");

        when(predictionService.backtestIntradaySignals(any())).thenReturn(
                intradayPrediction(List.of(point(0, null))));
        assertThatThrownBy(() -> service.runIntraday(intraday(body))).hasMessageContaining("Invalid intraday");

        when(predictionService.backtestIntradaySignals(any())).thenReturn(
                intradayPrediction(List.of(point(0, "BUY"), point(0, "SELL"))));
        assertThatThrownBy(() -> service.runIntraday(intraday(body))).hasMessageContaining("Duplicate");

        when(predictionService.backtestIntradaySignals(any())).thenReturn(
                intradayPrediction(List.of(point(0, "BUY"))));
        assertThatThrownBy(() -> service.runIntraday(intraday(body))).hasMessageContaining("Missing prediction");
    }

    @Test
    void runIntraday_chronosLimitedToSixtyBars() {
        Instant end = T0.plusSeconds(61 * 60);
        when(intradayOhlcvService.getIntraday("BTCUSDT", "1m", T0, end)).thenReturn(minuteBars(T0, 61));

        assertThatThrownBy(() -> service.runIntraday(intraday(
                "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"PREDICTION\",\"params\":{\"model\":\"CHRONOS_BOLT\"},"
                        + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T01:01:00Z\"}")))
                .hasMessageContaining("Chronos-Bolt");
    }

    // ---------- performance reports ----------

    @Test
    void inspectPerformanceReportUniverse_returnsEligibleSymbols() {
        when(runRepository.findEligibleCryptoSymbols("BINANCE", FROM, TO, 60)).thenReturn(List.of("BTCUSDT", "ETHUSDT"));
        when(runRepository.countKnownCryptoSymbols("BINANCE")).thenReturn(30);

        PerformanceReportUniverseResponse r = service.inspectPerformanceReportUniverse(FROM, TO, 60);

        assertThat(r.eligibleSymbols()).containsExactly("BTCUSDT", "ETHUSDT");
        assertThatThrownBy(() -> service.inspectPerformanceReportUniverse(FROM, TO, 10))
                .hasMessageContaining("between 50 and 500");
    }

    @Test
    void generatePerformanceReport_aggregatesSuccessAndFailureRows() {
        // BTC: 일봉 데이터 있음(BUY_AND_HOLD 성공), 예측은 이력 부족으로 실패. ETH: 데이터 없음.
        when(runRepository.loadBars(eq("BTCUSDT"), anyString(), any(), any())).thenReturn(dailyBars(10));
        when(runRepository.loadBars(eq("ETHUSDT"), anyString(), any(), any())).thenReturn(List.of());
        when(runRepository.countBarsBefore(anyString(), anyString(), any())).thenReturn(0);
        stubSuccessfulSave(1L);
        PerformanceReportRequest request = json(PerformanceReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"symbols\":[\" btcusdt\",\"BTCUSDT\",\"ETHUSDT\",\"\"]}");

        PerformanceReportResponse response = service.generatePerformanceReport(request);

        assertThat(response.rows()).hasSize(8); // 2 symbols x (B&H + 3 models)
        PerformanceReportRow bh = response.rows().get(0);
        assertThat(bh.status()).isEqualTo("SUCCESS");
        assertThat(bh.mae()).isEqualByComparingTo("1.5");
        assertThat(bh.buySignalCount()).isEqualTo(3);
        assertThat(response.rows().get(1).status()).isEqualTo("FAILED");
        assertThat(response.rows().get(1).errorSummary()).contains("at least 50");
        assertThat(response.summaries()).hasSize(4);
        assertThat(response.summaries().get(0).successfulRuns()).isEqualTo(1);
        assertThat(response.summaries().get(0).failedRuns()).isEqualTo(1);
        assertThat(response.summaries().get(0).positiveReturnCount()).isEqualTo(1);
        assertThat(response.summaries().get(0).averageTotalReturnPct()).isEqualByComparingTo("10");
        assertThat(response.summaries().get(1).averageTotalReturnPct()).isNull();
    }

    @Test
    void generatePerformanceReport_usesDefaultSymbolsAndValidates() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(List.of());
        PerformanceReportRequest request = json(PerformanceReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}");
        assertThat(service.generatePerformanceReport(request).rows()).hasSize(40);

        assertThatThrownBy(() -> service.generatePerformanceReport(json(PerformanceReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"initialCash\":0}")))
                .hasMessageContaining("initialCash must be positive");
        assertThatThrownBy(() -> service.generatePerformanceReport(json(PerformanceReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"symbols\":[\" \"]}")))
                .hasMessageContaining("at least one symbol");
    }

    @Test
    void generateThresholdReport_comparesMultipliers() {
        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(List.of());
        ThresholdReportRequest request = json(ThresholdReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"model\":\" chronos_bolt \",\"symbols\":[\"BTCUSDT\"]}");

        ThresholdReportResponse response = service.generateThresholdReport(request);

        assertThat(response.model()).isEqualTo("CHRONOS_BOLT");
        assertThat(response.summaries()).hasSize(3);
        assertThat(response.summaries().get(0).failedRuns()).isEqualTo(1);

        assertThatThrownBy(() -> service.generateThresholdReport(json(ThresholdReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"model\":\"NOPE\"}")))
                .hasMessageContaining("unsupported prediction model");
        assertThatThrownBy(() -> service.generateThresholdReport(json(ThresholdReportRequest.class,
                "{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"model\":\"ARIMA\",\"initialCash\":-5}")))
                .hasMessageContaining("initialCash must be positive");
    }

    @Test
    void reportHelpers_exposeDefaults() {
        assertThat(service.reportModels()).containsExactly("ARIMA", "LOG_RETURN_ARIMA", "CHRONOS_BOLT");
        Map<String, Object> params = service.defaultReportPredictionParams("ARIMA");
        assertThat(params).containsEntry("model", "ARIMA").containsEntry("warmup", 50);
        assertThat(service.summarizePerformanceReportRows(List.of())).hasSize(4);

        when(runRepository.loadBars(anyString(), anyString(), any(), any())).thenReturn(List.of());
        PerformanceReportRow row = service.runPerformanceReportRow(
                "BTCUSDT", StrategyType.BUY_AND_HOLD, null, Map.of(), BigDecimal.TEN, FROM, TO);
        assertThat(row.status()).isEqualTo("FAILED");
    }

    // ---------- query methods ----------

    @Test
    void queryMethods_mapRepositoryRows() {
        when(runRepository.findRun(3L)).thenReturn(Optional.of(runRow(3L)));
        when(runRepository.findRunsByStrategy(4L)).thenReturn(List.of(runRow(1L), runRow(2L)));
        when(runRepository.runExists(3L)).thenReturn(true);
        when(runRepository.runExists(404L)).thenReturn(false);
        when(runRepository.findTrades(3L)).thenReturn(List.of(new TradeRow(
                1, FROM, "BUY", BigDecimal.ONE, BigDecimal.ONE, BigDecimal.ZERO, BigDecimal.TEN, null)));
        when(runRepository.findEquityCurve(3L)).thenReturn(List.of(new EquityRow(FROM, BigDecimal.TEN, BigDecimal.ZERO)));
        when(runRepository.findPredictionPoints(3L)).thenReturn(List.of(new PredictionPointRow(
                FROM, FROM.plusDays(1), BigDecimal.ONE, BigDecimal.valueOf(2), BigDecimal.ONE, BigDecimal.ONE, "BUY")));

        assertThat(service.getRun(3L)).isPresent();
        assertThat(service.getRun(999L)).isEmpty();
        assertThat(service.getRunsByStrategy(4L)).hasSize(2);
        assertThat(service.getTrades(3L).orElseThrow()).hasSize(1);
        assertThat(service.getTrades(3L).orElseThrow().get(0).getSide()).isEqualTo("BUY");
        assertThat(service.getEquityCurve(3L).orElseThrow()).hasSize(1);
        assertThat(service.getPredictionPoints(3L).orElseThrow().get(0).getSignal()).isEqualTo("BUY");
        assertThat(service.getTrades(404L)).isEmpty();
        assertThat(service.getEquityCurve(404L)).isEmpty();
        assertThat(service.getPredictionPoints(404L)).isEmpty();
        verify(runRepository, never()).findTrades(404L);
        verify(runRepository, never()).loadBars(anyString(), anyString(), any(), any());
        verify(intradayOhlcvService, never()).getIntraday(anyString(), anyString(), any(), any());
    }
}
