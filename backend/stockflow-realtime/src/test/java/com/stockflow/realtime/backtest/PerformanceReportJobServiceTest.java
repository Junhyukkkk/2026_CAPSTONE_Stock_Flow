package com.stockflow.realtime.backtest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.stockflow.realtime.backtest.dto.PerformanceReportJobRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportJobResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.PerformanceReportUniverseResponse;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.PerformanceReportJobRepository;
import com.stockflow.realtime.backtest.repository.PerformanceReportJobRepository.JobRow;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PerformanceReportJobServiceTest {

    private static final LocalDate FROM = LocalDate.of(2025, 1, 1);
    private static final LocalDate TO = LocalDate.of(2025, 3, 1);

    @Mock BacktestRunService runService;
    @Mock PerformanceReportJobRepository jobRepository;

    /** 작업 실행기를 호출 스레드에서 즉시 실행해 비동기 경로를 결정적으로 만든다. */
    PerformanceReportJobService service;
    final List<Runnable> queued = new ArrayList<>();

    @BeforeEach
    void setUp() {
        service = new PerformanceReportJobService(runService, jobRepository, queued::add);
    }

    private static PerformanceReportJobRequest request(String json) {
        try {
            return new ObjectMapper().registerModule(new JavaTimeModule())
                    .readValue(json, PerformanceReportJobRequest.class);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final String BODY = "{\"from\":\"2025-01-01\",\"to\":\"2025-03-01\"}";

    private static PerformanceReportRow row(String status) {
        return new PerformanceReportRow("BTCUSDT", "BUY_AND_HOLD", null, status, 1L, BigDecimal.TEN,
                null, null, null, null, null, null, null, null, null, null);
    }

    private void stubJob() {
        when(jobRepository.findJob(1L)).thenReturn(Optional.of(new JobRow(1L, "QUEUED", FROM, TO,
                BigDecimal.TEN, 50, 2, 0, 0, 0, Instant.EPOCH, null, null, null)));
        when(jobRepository.findItems(1L)).thenReturn(List.of(row("SUCCESS")));
        when(runService.summarizePerformanceReportRows(any())).thenReturn(List.of());
    }

    @Test
    void startQueuesJobAndReturnsItsState() {
        when(jobRepository.hasActiveJob()).thenReturn(false);
        when(runService.inspectPerformanceReportUniverse(FROM, TO, 50)).thenReturn(
                new PerformanceReportUniverseResponse(FROM, TO, 50, 5, 2, List.of("BTCUSDT", "ETHUSDT")));
        when(jobRepository.createJob(eq(FROM), eq(TO), any(), eq(50), eq(2))).thenReturn(1L);
        stubJob();

        PerformanceReportJobResponse response = service.start(request(BODY));

        assertThat(response.id()).isEqualTo(1L);
        assertThat(response.status()).isEqualTo("QUEUED");
        assertThat(response.rows()).hasSize(1);
        assertThat(queued).hasSize(1);
        verify(jobRepository, never()).markRunning(anyLong());
    }

    @Test
    void queuedJobRunsEverySymbolAndMarksSuccess() {
        when(jobRepository.hasActiveJob()).thenReturn(false);
        when(runService.inspectPerformanceReportUniverse(any(), any(), anyInt())).thenReturn(
                new PerformanceReportUniverseResponse(FROM, TO, 50, 5, 2, List.of("BTCUSDT", "ETHUSDT")));
        when(jobRepository.createJob(any(), any(), any(), anyInt(), anyInt())).thenReturn(1L);
        when(runService.reportModels()).thenReturn(List.of("ARIMA", "CHRONOS_BOLT"));
        when(runService.defaultReportPredictionParams(anyString())).thenReturn(Map.of());
        when(runService.runPerformanceReportRow(anyString(), eq(StrategyType.BUY_AND_HOLD), any(), anyMap(), any(), any(), any()))
                .thenReturn(row("SUCCESS"));
        when(runService.runPerformanceReportRow(anyString(), eq(StrategyType.PREDICTION), anyString(), anyMap(), any(), any(), any()))
                .thenReturn(row("SUCCESS"), row("FAILED"));
        stubJob();

        service.start(request("{\"from\":\"2025-01-01\",\"to\":\"2025-03-01\",\"initialCash\":500}"));
        queued.get(0).run();

        verify(jobRepository).markRunning(1L);
        verify(jobRepository, times(2 * 3)).saveItem(eq(1L), any());
        verify(jobRepository).markSymbolComplete(1L, 2, 1);
        verify(jobRepository).markSucceeded(1L);
        verify(jobRepository, never()).markFailed(anyLong(), any());
    }

    @Test
    void unexpectedErrorMarksJobFailed() {
        when(jobRepository.hasActiveJob()).thenReturn(false);
        when(runService.inspectPerformanceReportUniverse(any(), any(), anyInt())).thenReturn(
                new PerformanceReportUniverseResponse(FROM, TO, 50, 1, 1, List.of("BTCUSDT")));
        when(jobRepository.createJob(any(), any(), any(), anyInt(), anyInt())).thenReturn(1L);
        when(runService.runPerformanceReportRow(anyString(), any(), any(), anyMap(), any(), any(), any()))
                .thenThrow(new IllegalStateException("db down"));
        stubJob();

        service.start(request(BODY));
        queued.get(0).run();

        verify(jobRepository).markFailed(1L, "db down");
        verify(jobRepository, never()).markSucceeded(anyLong());
    }

    @Test
    void startRejectsInvalidStates() {
        assertThatThrownBy(() -> service.start(request(
                "{\"from\":\"2025-01-01\",\"to\":\"2025-03-01\",\"initialCash\":0}")))
                .hasMessageContaining("initialCash must be positive");

        when(jobRepository.hasActiveJob()).thenReturn(true);
        assertThatThrownBy(() -> service.start(request(BODY)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("already running");

        when(jobRepository.hasActiveJob()).thenReturn(false);
        when(runService.inspectPerformanceReportUniverse(any(), any(), anyInt())).thenReturn(
                new PerformanceReportUniverseResponse(FROM, TO, 50, 5, 0, List.of()));
        assertThatThrownBy(() -> service.start(request(BODY)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No eligible crypto symbols");
        verify(jobRepository, never()).createJob(any(), any(), any(), anyInt(), anyInt());
    }

    @Test
    void getReturnsEmptyForUnknownJob() {
        when(jobRepository.findJob(9L)).thenReturn(Optional.empty());

        assertThat(service.get(9L)).isEmpty();
    }
}
