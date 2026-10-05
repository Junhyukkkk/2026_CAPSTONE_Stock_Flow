package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.BacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.EquityPointResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportJobResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportUniverseResponse;
import com.stockflow.realtime.backtest.dto.PredictionPointResponse;
import com.stockflow.realtime.backtest.dto.StrategyResponse;
import com.stockflow.realtime.backtest.dto.ThresholdReportResponse;
import com.stockflow.realtime.backtest.dto.TradeResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class BacktestControllerTest {

    @Mock BacktestStrategyService strategyService;
    @Mock BacktestRunService runService;
    @Mock PerformanceReportJobService jobService;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new BacktestController(strategyService, runService, jobService))
                .setControllerAdvice(new BacktestExceptionHandler())
                .build();
    }

    private static StrategyResponse strategy(long id) {
        return StrategyResponse.builder().id(id).name("n").symbol("BTCUSDT").strategyType("RSI")
                .params(Map.of()).initialCash(BigDecimal.TEN).build();
    }

    private static BacktestRunResponse run(long id) {
        return BacktestRunResponse.builder().id(id).symbol("BTCUSDT").status("SUCCESS").build();
    }

    private static final String STRATEGY_BODY = "{\"name\":\"n\",\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\"}";

    @Test
    void createStrategyValidatesBodyAndDelegates() throws Exception {
        when(strategyService.create(any())).thenReturn(strategy(1));

        mvc.perform(post("/api/backtest/strategies").contentType(MediaType.APPLICATION_JSON).content(STRATEGY_BODY))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(1));
        mvc.perform(post("/api/backtest/strategies").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void createStrategyMapsServiceValidationTo400() throws Exception {
        when(strategyService.create(any())).thenThrow(new IllegalArgumentException("bad type"));

        mvc.perform(post("/api/backtest/strategies").contentType(MediaType.APPLICATION_JSON).content(STRATEGY_BODY))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.detail").value("bad type"));
    }

    @Test
    void strategyReadUpdateDelete() throws Exception {
        when(strategyService.list("BTC")).thenReturn(List.of(strategy(1), strategy(2)));
        when(strategyService.get(1L)).thenReturn(Optional.of(strategy(1)));
        when(strategyService.get(9L)).thenReturn(Optional.empty());
        when(strategyService.update(eq(1L), any())).thenReturn(Optional.of(strategy(1)));
        when(strategyService.update(eq(9L), any())).thenReturn(Optional.empty());
        when(strategyService.delete(1L)).thenReturn(true);
        when(strategyService.delete(9L)).thenReturn(false);

        mvc.perform(get("/api/backtest/strategies").param("symbol", "BTC")).andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
        mvc.perform(get("/api/backtest/strategies/1")).andExpect(status().isOk());
        mvc.perform(get("/api/backtest/strategies/9")).andExpect(status().isNotFound());
        mvc.perform(put("/api/backtest/strategies/1").contentType(MediaType.APPLICATION_JSON).content(STRATEGY_BODY))
                .andExpect(status().isOk());
        mvc.perform(put("/api/backtest/strategies/9").contentType(MediaType.APPLICATION_JSON).content(STRATEGY_BODY))
                .andExpect(status().isNotFound());
        mvc.perform(delete("/api/backtest/strategies/1")).andExpect(status().isNoContent());
        mvc.perform(delete("/api/backtest/strategies/9")).andExpect(status().isNotFound());
    }

    @Test
    void runEndpoints() throws Exception {
        LocalDate from = LocalDate.of(2025, 1, 1);
        LocalDate to = LocalDate.of(2025, 1, 10);
        when(runService.runSavedStrategy(1L, from, to)).thenReturn(Optional.of(run(5)));
        when(runService.runSavedStrategy(9L, from, to)).thenReturn(Optional.empty());
        when(runService.runAdHoc(any())).thenReturn(run(6));

        mvc.perform(post("/api/backtest/strategies/1/run").param("from", "2025-01-01").param("to", "2025-01-10"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(5));
        mvc.perform(post("/api/backtest/strategies/9/run").param("from", "2025-01-01").param("to", "2025-01-10"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/api/backtest/run").contentType(MediaType.APPLICATION_JSON).content(
                        "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(6));
    }

    @Test
    void noDataFromServiceIs404() throws Exception {
        when(runService.runAdHoc(any())).thenThrow(new BacktestRunService.NoDataException("no bars"));

        mvc.perform(post("/api/backtest/run").contentType(MediaType.APPLICATION_JSON).content(
                        "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"RSI\",\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}"))
                .andExpect(status().isNotFound());
    }

    @Test
    void readinessEndpoints() throws Exception {
        LocalDate d = LocalDate.of(2025, 1, 1);
        Instant t = Instant.parse("2025-01-01T00:00:00Z");
        when(runService.inspectDataReadiness(eq("BTCUSDT"), any(), any(), eq("BINANCE"), eq(0)))
                .thenReturn(new BacktestDataReadinessResponse("BTCUSDT", "BINANCE", d, d, d, d, 1, 1, 0, 0, 0,
                        true, "READY", "ok"));
        when(runService.inspectIntradayDataReadiness(eq("BTCUSDT"), any(), any(), eq("1m"), eq("BINANCE"), eq(200)))
                .thenReturn(new IntradayBacktestDataReadinessResponse("BTCUSDT", "BINANCE", "1m", t, t, t, t,
                        1, 1, 0, 0, 200, true, "READY", "ok"));

        mvc.perform(get("/api/backtest/readiness").param("symbol", "BTCUSDT")
                        .param("from", "2025-01-01").param("to", "2025-01-01"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.status").value("READY"));
        mvc.perform(get("/api/backtest/intraday-readiness").param("symbol", "BTCUSDT")
                        .param("from", "2025-01-01T00:00:00Z").param("to", "2025-01-01T00:01:00Z"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.canUseForBacktest").value(true));
    }

    @Test
    void intradayRunAndReports() throws Exception {
        IntradayBacktestResponse intraday = new IntradayBacktestResponse("BTCUSDT", "BINANCE", "1m", "BUY_AND_HOLD",
                Map.of(), Instant.EPOCH, Instant.EPOCH, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE,
                BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.ZERO, 0, null, 0, List.of(), List.of(), List.of());
        when(runService.runIntraday(any())).thenReturn(intraday);
        LocalDate d = LocalDate.of(2025, 1, 1);
        when(runService.generatePerformanceReport(any())).thenReturn(
                new PerformanceReportResponse(d, d, BigDecimal.TEN, List.of(), List.of()));
        when(runService.inspectPerformanceReportUniverse(any(), any(), anyInt())).thenReturn(
                new PerformanceReportUniverseResponse(d, d, 50, 3, 1, List.of("BTCUSDT")));
        when(runService.generateThresholdReport(any())).thenReturn(
                new ThresholdReportResponse(d, d, BigDecimal.TEN, "ARIMA", List.of()));

        mvc.perform(post("/api/backtest/intraday/run").contentType(MediaType.APPLICATION_JSON).content(
                        "{\"symbol\":\"BTCUSDT\",\"strategyType\":\"BUY_AND_HOLD\","
                                + "\"from\":\"2025-01-01T00:00:00Z\",\"to\":\"2025-01-01T00:10:00Z\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.interval").value("1m"));
        mvc.perform(post("/api/backtest/performance-report").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}"))
                .andExpect(status().isOk());
        mvc.perform(get("/api/backtest/performance-report/universe")
                        .param("from", "2025-01-01").param("to", "2025-01-10"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.eligibleSymbolCount").value(1));
        mvc.perform(post("/api/backtest/performance-report/thresholds").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"model\":\"ARIMA\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.model").value("ARIMA"));
        mvc.perform(post("/api/backtest/performance-report/thresholds").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void performanceReportJobs() throws Exception {
        LocalDate d = LocalDate.of(2025, 1, 1);
        PerformanceReportJobResponse job = new PerformanceReportJobResponse(1, "QUEUED", d, d, BigDecimal.TEN, 50,
                3, 0, 0, 0, Instant.EPOCH, null, null, null, List.of(), List.of());
        when(jobService.start(any())).thenReturn(job);
        when(jobService.get(1L)).thenReturn(Optional.of(job));
        when(jobService.get(9L)).thenReturn(Optional.empty());

        mvc.perform(post("/api/backtest/performance-report/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\"}"))
                .andExpect(status().isAccepted()).andExpect(jsonPath("$.status").value("QUEUED"));
        mvc.perform(post("/api/backtest/performance-report/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"from\":\"2025-01-01\",\"to\":\"2025-01-10\",\"minimumHistoryDays\":5}"))
                .andExpect(status().isBadRequest());
        mvc.perform(get("/api/backtest/performance-report/jobs/1")).andExpect(status().isOk());
        mvc.perform(get("/api/backtest/performance-report/jobs/9")).andExpect(status().isNotFound());
    }

    @Test
    void runResultEndpoints() throws Exception {
        when(runService.getRunsByStrategy(1L)).thenReturn(List.of(run(1), run(2)));
        when(runService.getRun(1L)).thenReturn(Optional.of(run(1)));
        when(runService.getRun(9L)).thenReturn(Optional.empty());
        when(runService.getTrades(1L)).thenReturn(Optional.of(List.of(TradeResponse.builder().seq(1).side("BUY").build())));
        when(runService.getTrades(9L)).thenReturn(Optional.empty());
        when(runService.getEquityCurve(1L)).thenReturn(Optional.of(List.of(EquityPointResponse.builder().build())));
        when(runService.getEquityCurve(9L)).thenReturn(Optional.empty());
        when(runService.getPredictionPoints(1L)).thenReturn(Optional.of(List.of(PredictionPointResponse.builder().build())));
        when(runService.getPredictionPoints(9L)).thenReturn(Optional.empty());

        mvc.perform(get("/api/backtest/strategies/1/runs")).andExpect(jsonPath("$.length()").value(2));
        mvc.perform(get("/api/backtest/runs/1")).andExpect(status().isOk());
        mvc.perform(get("/api/backtest/runs/9")).andExpect(status().isNotFound());
        mvc.perform(get("/api/backtest/runs/1/trades")).andExpect(status().isOk()).andExpect(jsonPath("$[0].side").value("BUY"));
        mvc.perform(get("/api/backtest/runs/9/trades")).andExpect(status().isNotFound());
        mvc.perform(get("/api/backtest/runs/1/equity-curve")).andExpect(status().isOk());
        mvc.perform(get("/api/backtest/runs/9/equity-curve")).andExpect(status().isNotFound());
        mvc.perform(get("/api/backtest/runs/1/prediction-points")).andExpect(status().isOk());
        mvc.perform(get("/api/backtest/runs/9/prediction-points")).andExpect(status().isNotFound());
    }
}
