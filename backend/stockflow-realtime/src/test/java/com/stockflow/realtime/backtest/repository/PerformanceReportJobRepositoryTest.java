package com.stockflow.realtime.backtest.repository;

import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.repository.PerformanceReportJobRepository.JobRow;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class PerformanceReportJobRepositoryTest {

    private static final LocalDate D1 = LocalDate.of(2025, 1, 1);

    private JdbcTemplate jdbc;
    private PerformanceReportJobRepository repository;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        repository = new PerformanceReportJobRepository(jdbc);
    }

    private void stubRows(List<Map<String, Object>> rows) {
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), rows));
    }

    @Test
    void hasActiveJobTreatsNullAsFalse() {
        when(jdbc.queryForObject(anyString(), eq(Boolean.class))).thenReturn(true, false, null);

        assertThat(repository.hasActiveJob()).isTrue();
        assertThat(repository.hasActiveJob()).isFalse();
        assertThat(repository.hasActiveJob()).isFalse();
    }

    @Test
    void createJobReturnsGeneratedIdOrFails() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(5L, (Long) null);

        assertThat(repository.createJob(D1, D1, BigDecimal.TEN, 50, 3)).isEqualTo(5L);
        assertThatThrownBy(() -> repository.createJob(D1, D1, BigDecimal.TEN, 50, 3))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void stateTransitionsIssueUpdates() {
        repository.markRunning(1L);
        repository.markSymbolComplete(1L, 3, 1);
        repository.markSucceeded(1L);
        repository.markFailed(1L, "x".repeat(5000));
        repository.markFailed(1L, null);

        verify(jdbc).update(contains("'RUNNING'"), eq(1L));
        verify(jdbc).update(contains("completed_symbols"), eq(3), eq(1), eq(1L));
        verify(jdbc).update(contains("'SUCCEEDED'"), eq(1L));
        verify(jdbc).update(contains("'FAILED'"), eq("x".repeat(4000)), eq(1L));
        verify(jdbc).update(contains("'FAILED'"), eq((String) null), eq(1L));
    }

    @Test
    void saveItemStoresEmptyStringForMissingModel() {
        PerformanceReportRow row = new PerformanceReportRow("BTCUSDT", "BUY_AND_HOLD", null, "FAILED", null,
                null, null, null, null, null, null, null, null, null, null, "e".repeat(5000));

        repository.saveItem(1L, row);

        verify(jdbc).update(contains("backtest_performance_report_items"), eq(1L), eq("BTCUSDT"),
                eq("BUY_AND_HOLD"), eq(""), eq("FAILED"), any(), any(), any(), any(), any(), any(), any(),
                any(), any(), any(), any(), eq("e".repeat(4000)));
    }

    @Test
    void findJobMapsRowsAndOptionalTimestamps() {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1L); row.put("status", "RUNNING"); row.put("from_date", D1); row.put("to_date", D1);
        row.put("initial_cash", "100"); row.put("minimum_history_days", 50); row.put("total_symbols", 10);
        row.put("completed_symbols", 2); row.put("successful_rows", 6); row.put("failed_rows", 2);
        row.put("created_at", Instant.EPOCH); row.put("started_at", Instant.EPOCH.plusSeconds(5));
        row.put("error_summary", null);
        stubRows(List.of(row));

        JobRow job = repository.findJob(1L).orElseThrow();
        assertThat(job.status()).isEqualTo("RUNNING");
        assertThat(job.startedAt()).isEqualTo(Instant.EPOCH.plusSeconds(5));
        assertThat(job.finishedAt()).isNull();
        assertThat(job.totalSymbols()).isEqualTo(10);

        stubRows(List.of());
        assertThat(repository.findJob(2L)).isEmpty();
    }

    @Test
    void findItemsMapsBlankModelToNull() {
        Map<String, Object> row = new HashMap<>();
        row.put("symbol", "BTCUSDT"); row.put("strategy_type", "BUY_AND_HOLD"); row.put("model", "");
        row.put("status", "SUCCESS"); row.put("run_id", 3L); row.put("total_return_pct", "1.5");
        row.put("buy_signal_count", 2); row.put("trade_count", 4);
        Map<String, Object> withModel = new HashMap<>(row);
        withModel.put("model", "ARIMA");
        stubRows(List.of(row, withModel));

        List<PerformanceReportRow> rows = repository.findItems(1L);

        assertThat(rows).hasSize(2);
        assertThat(rows.get(0).model()).isNull();
        assertThat(rows.get(1).model()).isEqualTo("ARIMA");
        assertThat(rows.get(0).runId()).isEqualTo(3L);
        assertThat(rows.get(0).buySignalCount()).isEqualTo(2);
        assertThat(rows.get(0).totalReturnPct()).isEqualByComparingTo("1.5");
    }
}
