package com.stockflow.realtime.backtest.repository;

import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

/** 전체 성과 리포트의 작업 헤더와 종목별 결과를 저장한다. */
@Repository
@RequiredArgsConstructor
public class PerformanceReportJobRepository {

    private final JdbcTemplate jdbcTemplate;

    public boolean hasActiveJob() {
        Boolean exists = jdbcTemplate.queryForObject(
                "SELECT EXISTS (SELECT 1 FROM backtest_performance_report_jobs WHERE status IN ('QUEUED', 'RUNNING'))",
                Boolean.class);
        return Boolean.TRUE.equals(exists);
    }

    public long createJob(LocalDate from, LocalDate to, BigDecimal initialCash,
                          int minimumHistoryDays, int totalSymbols) {
        Long id = jdbcTemplate.queryForObject(
                """
                INSERT INTO backtest_performance_report_jobs
                    (from_date, to_date, initial_cash, minimum_history_days, total_symbols, status)
                VALUES (?, ?, ?, ?, ?, 'QUEUED')
                RETURNING id
                """,
                Long.class, Date.valueOf(from), Date.valueOf(to), initialCash,
                minimumHistoryDays, totalSymbols);
        if (id == null) {
            throw new IllegalStateException("Failed to create performance report job");
        }
        return id;
    }

    public void markRunning(long jobId) {
        jdbcTemplate.update(
                "UPDATE backtest_performance_report_jobs SET status = 'RUNNING', started_at = NOW() WHERE id = ?",
                jobId);
    }

    public void saveItem(long jobId, PerformanceReportRow row) {
        jdbcTemplate.update(
                """
                INSERT INTO backtest_performance_report_items
                    (job_id, symbol, strategy_type, model, status, run_id, total_return_pct, mdd_pct,
                     mae, rmse, mae_pct, rmse_pct, buy_signal_count, hold_signal_count, sell_signal_count,
                     trade_count, error_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                jobId, row.symbol(), row.strategyType(), row.model() == null ? "" : row.model(), row.status(),
                row.runId(), row.totalReturnPct(), row.mddPct(), row.mae(), row.rmse(), row.maePct(),
                row.rmsePct(), row.buySignalCount(), row.holdSignalCount(), row.sellSignalCount(),
                row.tradeCount(), truncate(row.errorSummary()));
    }

    public void markSymbolComplete(long jobId, int successfulRows, int failedRows) {
        jdbcTemplate.update(
                """
                UPDATE backtest_performance_report_jobs
                SET completed_symbols = completed_symbols + 1,
                    successful_rows = successful_rows + ?,
                    failed_rows = failed_rows + ?
                WHERE id = ?
                """,
                successfulRows, failedRows, jobId);
    }

    public void markSucceeded(long jobId) {
        jdbcTemplate.update(
                "UPDATE backtest_performance_report_jobs SET status = 'SUCCEEDED', finished_at = NOW() WHERE id = ?",
                jobId);
    }

    public void markFailed(long jobId, String error) {
        jdbcTemplate.update(
                """
                UPDATE backtest_performance_report_jobs
                SET status = 'FAILED', error_summary = ?, finished_at = NOW()
                WHERE id = ?
                """,
                truncate(error), jobId);
    }

    public Optional<JobRow> findJob(long jobId) {
        List<JobRow> rows = jdbcTemplate.query(
                "SELECT * FROM backtest_performance_report_jobs WHERE id = ?",
                (rs, rowNum) -> new JobRow(
                        rs.getLong("id"), rs.getString("status"),
                        rs.getDate("from_date").toLocalDate(), rs.getDate("to_date").toLocalDate(),
                        rs.getBigDecimal("initial_cash"), rs.getInt("minimum_history_days"),
                        rs.getInt("total_symbols"), rs.getInt("completed_symbols"),
                        rs.getInt("successful_rows"), rs.getInt("failed_rows"),
                        rs.getTimestamp("created_at").toInstant(),
                        toInstant(rs, "started_at"), toInstant(rs, "finished_at"),
                        rs.getString("error_summary")),
                jobId);
        return rows.stream().findFirst();
    }

    public List<PerformanceReportRow> findItems(long jobId) {
        return jdbcTemplate.query(
                """
                SELECT symbol, strategy_type, model, status, run_id, total_return_pct, mdd_pct,
                       mae, rmse, mae_pct, rmse_pct, buy_signal_count, hold_signal_count,
                       sell_signal_count, trade_count, error_summary
                FROM backtest_performance_report_items
                WHERE job_id = ?
                ORDER BY symbol, strategy_type, model
                """,
                (rs, rowNum) -> new PerformanceReportRow(
                        rs.getString("symbol"), rs.getString("strategy_type"),
                        emptyToNull(rs.getString("model")), rs.getString("status"),
                        (Long) rs.getObject("run_id"), rs.getBigDecimal("total_return_pct"),
                        rs.getBigDecimal("mdd_pct"), rs.getBigDecimal("mae"), rs.getBigDecimal("rmse"),
                        rs.getBigDecimal("mae_pct"), rs.getBigDecimal("rmse_pct"),
                        (Integer) rs.getObject("buy_signal_count"), (Integer) rs.getObject("hold_signal_count"),
                        (Integer) rs.getObject("sell_signal_count"), (Integer) rs.getObject("trade_count"),
                        rs.getString("error_summary")),
                jobId);
    }

    private Instant toInstant(java.sql.ResultSet rs, String column) throws java.sql.SQLException {
        var value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private String emptyToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private String truncate(String value) {
        return value != null && value.length() > 4000 ? value.substring(0, 4000) : value;
    }

    public record JobRow(
            long id, String status, LocalDate fromDate, LocalDate toDate, BigDecimal initialCash,
            int minimumHistoryDays, int totalSymbols, int completedSymbols, int successfulRows,
            int failedRows, Instant createdAt, Instant startedAt, Instant finishedAt, String errorSummary
    ) {
    }
}
