package com.stockflow.realtime.controller;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.batch.service.BinanceDailyOhlcvBackfillService;
import com.stockflow.realtime.batch.service.BinanceDailyOhlcvBackfillService.BackfillResult;
import com.stockflow.realtime.batch.service.PrevCloseSyncService;
import com.stockflow.realtime.service.RedisPriceService;
import com.stockflow.realtime.stock.IndicatorHistoryService;
import com.stockflow.realtime.stock.InstrumentService;
import com.stockflow.realtime.stock.IntradayOhlcvService;
import com.stockflow.realtime.stock.OhlcvHistoryService;
import com.stockflow.realtime.stock.dto.IndicatorResponse;
import com.stockflow.realtime.stock.dto.InstrumentResponse;
import com.stockflow.realtime.stock.dto.IntradayOhlcvResponse;
import com.stockflow.realtime.stock.dto.OhlcvResponse;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class ControllersTest {

    // ---------- PriceController ----------

    @Test
    void priceController() throws Exception {
        RedisPriceService redis = mock(RedisPriceService.class);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new PriceController(redis)).build();
        when(redis.getActivePrices()).thenReturn(List.of(PriceSnapshot.builder().symbol("A").build()));
        when(redis.getLatestPrice("BTC")).thenReturn(PriceSnapshot.builder().symbol("BTC").price(BigDecimal.TEN).build());
        when(redis.getPreviousClose("BTC")).thenReturn(BigDecimal.valueOf(9));

        mvc.perform(get("/api/price/active")).andExpect(status().isOk()).andExpect(jsonPath("$[0].symbol").value("A"));
        mvc.perform(get("/api/price/btc")).andExpect(status().isOk()).andExpect(jsonPath("$.symbol").value("BTC"));
        mvc.perform(get("/api/price/eth")).andExpect(status().isNotFound());
        mvc.perform(get("/api/price/btc/prev-close")).andExpect(status().isOk())
                .andExpect(jsonPath("$.previousClose").value(9));
        mvc.perform(get("/api/price/eth/prev-close")).andExpect(status().isNotFound());

        mvc.perform(put("/api/price/btc/prev-close").contentType(MediaType.APPLICATION_JSON).content("{\"price\": 12.5}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.symbol").value("BTC"));
        verify(redis).setPreviousClose("BTC", new BigDecimal("12.5"));
        mvc.perform(put("/api/price/btc/prev-close").contentType(MediaType.APPLICATION_JSON).content("{\"price\": 0}"))
                .andExpect(status().isBadRequest());
        mvc.perform(put("/api/price/btc/prev-close").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    // ---------- StockController ----------

    @Test
    void stockController() throws Exception {
        InstrumentService instruments = mock(InstrumentService.class);
        OhlcvHistoryService ohlcv = mock(OhlcvHistoryService.class);
        IntradayOhlcvService intraday = mock(IntradayOhlcvService.class);
        IndicatorHistoryService indicators = mock(IndicatorHistoryService.class);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new StockController(instruments, ohlcv, intraday, indicators)).build();
        InstrumentResponse btc = InstrumentResponse.builder().symbol("BTCUSDT").build();
        when(instruments.list("CRYPTO", true)).thenReturn(List.of(btc));
        when(instruments.getBySymbol("BTCUSDT")).thenReturn(Optional.of(btc));
        when(instruments.getBySymbol("NOPE")).thenReturn(Optional.empty());
        when(instruments.create(any())).thenReturn(btc);
        when(instruments.update(eq("BTCUSDT"), any())).thenReturn(Optional.of(btc));
        when(instruments.update(eq("NOPE"), any())).thenReturn(Optional.empty());
        when(instruments.deactivate("BTCUSDT")).thenReturn(true);
        when(instruments.deactivate("NOPE")).thenReturn(false);
        when(ohlcv.getOhlcv(eq("BTCUSDT"), any(), any()))
                .thenReturn(List.of(OhlcvResponse.builder().symbol("BTCUSDT").build()));
        when(intraday.getIntraday(eq("BTCUSDT"), eq("5m"), any(), any()))
                .thenReturn(List.of(IntradayOhlcvResponse.builder().symbol("BTCUSDT").build()));
        when(indicators.getIndicators(eq("BTCUSDT"), any(), any()))
                .thenReturn(List.of(IndicatorResponse.builder().symbol("BTCUSDT").build()));

        mvc.perform(get("/api/stocks").param("marketType", "CRYPTO")).andExpect(status().isOk())
                .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"));
        mvc.perform(get("/api/stocks/BTCUSDT")).andExpect(status().isOk());
        mvc.perform(get("/api/stocks/NOPE")).andExpect(status().isNotFound());
        String body = "{\"symbol\":\"BTCUSDT\",\"marketType\":\"CRYPTO\",\"exchange\":\"BINANCE\"}";
        mvc.perform(post("/api/stocks").contentType(MediaType.APPLICATION_JSON).content(body)).andExpect(status().isOk());
        mvc.perform(put("/api/stocks/BTCUSDT").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isOk());
        mvc.perform(put("/api/stocks/NOPE").contentType(MediaType.APPLICATION_JSON).content("{\"name\":\"x\"}"))
                .andExpect(status().isNotFound());
        mvc.perform(delete("/api/stocks/BTCUSDT")).andExpect(status().isNoContent());
        mvc.perform(delete("/api/stocks/NOPE")).andExpect(status().isNotFound());
        mvc.perform(get("/api/stocks/BTCUSDT/ohlcv").param("from", "2025-01-01").param("to", "2025-01-02"))
                .andExpect(status().isOk());
        mvc.perform(get("/api/stocks/BTCUSDT/intraday")).andExpect(status().isOk());
        mvc.perform(get("/api/stocks/BTCUSDT/indicators").param("from", "2025-01-01").param("to", "2025-01-02"))
                .andExpect(status().isOk());
    }

    // ---------- IndicatorController ----------

    @Test
    void indicatorController() throws Exception {
        IndicatorHistoryService service = mock(IndicatorHistoryService.class);
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new IndicatorController(service)).build();
        IndicatorResponse one = IndicatorResponse.builder().symbol("BTC").build();
        when(service.getLatest("BTC")).thenReturn(Optional.of(one));
        when(service.getLatest("NOPE")).thenReturn(Optional.empty());
        when(service.getHistory("BTC", 30)).thenReturn(List.of(one));
        when(service.getHistory("NOPE", 30)).thenReturn(List.of());
        when(service.getIndicators(eq("BTC"), any(), any())).thenReturn(List.of(one));
        when(service.getIndicators(eq("NOPE"), any(), any())).thenReturn(List.of());

        mvc.perform(get("/api/indicators/BTC")).andExpect(status().isOk());
        mvc.perform(get("/api/indicators/NOPE")).andExpect(status().isNotFound());
        mvc.perform(get("/api/indicators/BTC/history")).andExpect(status().isOk());
        mvc.perform(get("/api/indicators/NOPE/history")).andExpect(status().isNotFound());
        mvc.perform(get("/api/indicators/BTC/range").param("from", "2025-01-01").param("to", "2025-01-02"))
                .andExpect(status().isOk());
        mvc.perform(get("/api/indicators/NOPE/range").param("from", "2025-01-01").param("to", "2025-01-02"))
                .andExpect(status().isNotFound());
    }

    // ---------- BatchTriggerController ----------

    @Test
    void batchTriggerController() throws Exception {
        PrevCloseSyncService prevClose = mock(PrevCloseSyncService.class);
        BinanceDailyOhlcvBackfillService backfill = mock(BinanceDailyOhlcvBackfillService.class);
        JobLauncher launcher = mock(JobLauncher.class);
        Job ohlcvJob = mock(Job.class);
        Job indicatorJob = mock(Job.class);
        Job validationJob = mock(Job.class);
        when(indicatorJob.getName()).thenReturn("dailyIndicatorJob");
        JobExecution completed = new JobExecution(1L);
        completed.setStatus(BatchStatus.COMPLETED);
        when(launcher.run(eq(ohlcvJob), any(JobParameters.class))).thenReturn(completed);
        when(launcher.run(eq(indicatorJob), any(JobParameters.class))).thenThrow(new IllegalStateException("locked"));
        when(launcher.run(eq(validationJob), any(JobParameters.class))).thenReturn(completed);
        when(prevClose.syncFromDailyOhlcv()).thenReturn(0, 3);
        LocalDate from = LocalDate.of(2025, 1, 1);
        when(backfill.backfill(eq(from), eq(from.plusDays(1)), any())).thenReturn(
                new BackfillResult(from, from.plusDays(1), 1, 1, 2, List.of()));
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new BatchTriggerController(
                prevClose, backfill, launcher, ohlcvJob, indicatorJob, validationJob)).build();

        mvc.perform(post("/api/batch/prev-close-sync")).andExpect(status().isOk())
                .andExpect(jsonPath("$.loadedSymbols").value(0));
        mvc.perform(post("/api/batch/prev-close-sync")).andExpect(status().isOk())
                .andExpect(jsonPath("$.loadedSymbols").value(3));
        mvc.perform(post("/api/batch/daily").param("date", "2025-01-01")).andExpect(status().isOk())
                .andExpect(jsonPath("$.targetDate").value("2025-01-01"))
                .andExpect(jsonPath("$.dailyOhlcvJob").value("COMPLETED"))
                .andExpect(jsonPath("$.dailyIndicatorJob").value("FAILED: locked"));
        mvc.perform(post("/api/batch/daily").param("date", " ")).andExpect(status().isOk())
                .andExpect(jsonPath("$.targetDate").value(LocalDate.now().minusDays(1).toString()));
        mvc.perform(post("/api/batch/daily/backfill/binance").param("from", "2025-01-01").param("to", "2025-01-02"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.writtenBarCount").value(2));
    }

    // ---------- DevController ----------

    @Test
    void devController() throws Exception {
        MockMvc mvc = MockMvcBuilders.standaloneSetup(new DevController()).build();

        mvc.perform(post("/api/dev/log-error")).andExpect(status().isOk()).andExpect(jsonPath("$.level").value("ERROR"));
        mvc.perform(post("/api/dev/log-warn").param("message", "hi")).andExpect(status().isOk())
                .andExpect(jsonPath("$.message").value("hi"));
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> mvc.perform(post("/api/dev/raise-error")));
    }
}
