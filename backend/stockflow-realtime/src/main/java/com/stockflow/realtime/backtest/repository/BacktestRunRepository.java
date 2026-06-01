package com.stockflow.realtime.backtest.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.BacktestResult;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 백테스트 실행 결과(runs/trades/equity_curve) 저장·조회 및 일봉 입력 로딩.
 */
@Repository
@RequiredArgsConstructor
public class BacktestRunRepository {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 백테스트 입력 일봉 로딩. symbol_daily_ohlcv 에 source 가 여러 개일 수 있으므로
     * 일자별로 가장 최근 계산본(computed_at DESC) 하나만 선택한다.
     */
    public List<Bar> loadBars(String symbol, LocalDate from, LocalDate to) {
        return jdbcTemplate.query(
                """
                SELECT DISTINCT ON (trade_date)
                       trade_date, open, high, low, close, volume
                FROM symbol_daily_ohlcv
                WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date ASC, computed_at DESC
                """,
                (rs, rowNum) -> new Bar(
                        rs.getDate("trade_date").toLocalDate(),
                        rs.getBigDecimal("open"),
                        rs.getBigDecimal("high"),
                        rs.getBigDecimal("low"),
                        rs.getBigDecimal("close"),
                        rs.getBigDecimal("volume")),
                symbol.toUpperCase(), Date.valueOf(from), Date.valueOf(to));
    }

    /**
     * 실행 결과 헤더와 체결/자산곡선을 한 트랜잭션으로 저장하고 run id 를 반환한다.
     */
    public long saveResult(Long strategyId, String symbol, String strategyType,
                           Map<String, Object> params, LocalDate from, LocalDate to,
                           BacktestResult result) {
        Long runId = jdbcTemplate.queryForObject(
                """
                INSERT INTO backtest_runs
                    (strategy_id, symbol, strategy_type, params, from_date, to_date,
                     initial_cash, final_equity, total_return_pct, cagr_pct, mdd_pct,
                     trade_count, win_rate_pct, bar_count, status, finished_at)
                VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SUCCESS', NOW())
                RETURNING id
                """,
                Long.class,
                strategyId, symbol.toUpperCase(), strategyType, writeParams(params),
                Date.valueOf(from), Date.valueOf(to),
                result.initialCash(), result.finalEquity(), result.totalReturnPct(),
                result.cagrPct(), result.mddPct(), result.tradeCount(), result.winRatePct(),
                result.barCount());

        insertTrades(runId, result.trades());
        insertEquityCurve(runId, result.equityCurve());
        return runId;
    }

    /** 실패한 실행 기록 저장(분석/추적용). */
    public long saveFailure(Long strategyId, String symbol, String strategyType,
                            Map<String, Object> params, LocalDate from, LocalDate to,
                            BigDecimal initialCash, String error) {
        return jdbcTemplate.queryForObject(
                """
                INSERT INTO backtest_runs
                    (strategy_id, symbol, strategy_type, params, from_date, to_date,
                     initial_cash, status, error_summary, finished_at)
                VALUES (?, ?, ?, CAST(? AS jsonb), ?, ?, ?, 'FAILED', ?, NOW())
                RETURNING id
                """,
                Long.class,
                strategyId, symbol.toUpperCase(), strategyType, writeParams(params),
                Date.valueOf(from), Date.valueOf(to), initialCash,
                error != null && error.length() > 4000 ? error.substring(0, 4000) : error);
    }

    private void insertTrades(long runId, List<BacktestResult.Trade> trades) {
        if (trades.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO backtest_trades
                    (run_id, seq, trade_date, side, price, quantity, cash_after, equity_after, pnl_pct)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                trades, trades.size(),
                (ps, t) -> {
                    ps.setLong(1, runId);
                    ps.setInt(2, t.seq());
                    ps.setDate(3, Date.valueOf(t.date()));
                    ps.setString(4, t.side().name());
                    ps.setBigDecimal(5, t.price());
                    ps.setBigDecimal(6, t.quantity());
                    ps.setBigDecimal(7, t.cashAfter());
                    ps.setBigDecimal(8, t.equityAfter());
                    ps.setBigDecimal(9, t.pnlPct());
                });
    }

    private void insertEquityCurve(long runId, List<BacktestResult.EquityPoint> curve) {
        if (curve.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO backtest_equity_curve (run_id, trade_date, equity, drawdown_pct)
                VALUES (?, ?, ?, ?)
                """,
                curve, curve.size(),
                (ps, p) -> {
                    ps.setLong(1, runId);
                    ps.setDate(2, Date.valueOf(p.date()));
                    ps.setBigDecimal(3, p.equity());
                    ps.setBigDecimal(4, p.drawdownPct());
                });
    }

    public Optional<RunRow> findRun(long id) {
        List<RunRow> rows = jdbcTemplate.query(
                "SELECT * FROM backtest_runs WHERE id = ?", runRowMapper, id);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public List<RunRow> findRunsByStrategy(long strategyId) {
        return jdbcTemplate.query(
                "SELECT * FROM backtest_runs WHERE strategy_id = ? ORDER BY created_at DESC",
                runRowMapper, strategyId);
    }

    public boolean runExists(long id) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM backtest_runs WHERE id = ?", Integer.class, id);
        return count != null && count > 0;
    }

    public List<TradeRow> findTrades(long runId) {
        return jdbcTemplate.query(
                "SELECT * FROM backtest_trades WHERE run_id = ? ORDER BY seq ASC",
                (rs, rowNum) -> new TradeRow(
                        rs.getInt("seq"),
                        rs.getDate("trade_date").toLocalDate(),
                        rs.getString("side"),
                        rs.getBigDecimal("price"),
                        rs.getBigDecimal("quantity"),
                        rs.getBigDecimal("cash_after"),
                        rs.getBigDecimal("equity_after"),
                        rs.getBigDecimal("pnl_pct")),
                runId);
    }

    public List<EquityRow> findEquityCurve(long runId) {
        return jdbcTemplate.query(
                "SELECT * FROM backtest_equity_curve WHERE run_id = ? ORDER BY trade_date ASC",
                (rs, rowNum) -> new EquityRow(
                        rs.getDate("trade_date").toLocalDate(),
                        rs.getBigDecimal("equity"),
                        rs.getBigDecimal("drawdown_pct")),
                runId);
    }

    private final RowMapper<RunRow> runRowMapper = (rs, rowNum) -> new RunRow(
            rs.getLong("id"),
            rs.getObject("strategy_id") != null ? rs.getLong("strategy_id") : null,
            rs.getString("symbol"),
            rs.getString("strategy_type"),
            readParams(rs.getString("params")),
            rs.getDate("from_date").toLocalDate(),
            rs.getDate("to_date").toLocalDate(),
            rs.getBigDecimal("initial_cash"),
            rs.getBigDecimal("final_equity"),
            rs.getBigDecimal("total_return_pct"),
            rs.getBigDecimal("cagr_pct"),
            rs.getBigDecimal("mdd_pct"),
            rs.getObject("trade_count") != null ? rs.getInt("trade_count") : null,
            rs.getBigDecimal("win_rate_pct"),
            rs.getObject("bar_count") != null ? rs.getInt("bar_count") : null,
            rs.getString("status"),
            rs.getTimestamp("created_at").toInstant()
    );

    private Map<String, Object> readParams(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse run params JSON", e);
        }
    }

    private String writeParams(Map<String, Object> params) {
        try {
            return objectMapper.writeValueAsString(params == null ? Collections.emptyMap() : params);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize run params", e);
        }
    }

    public record RunRow(
            long id,
            Long strategyId,
            String symbol,
            String strategyType,
            Map<String, Object> params,
            LocalDate fromDate,
            LocalDate toDate,
            BigDecimal initialCash,
            BigDecimal finalEquity,
            BigDecimal totalReturnPct,
            BigDecimal cagrPct,
            BigDecimal mddPct,
            Integer tradeCount,
            BigDecimal winRatePct,
            Integer barCount,
            String status,
            Instant createdAt
    ) {
    }

    public record TradeRow(
            int seq,
            LocalDate tradeDate,
            String side,
            BigDecimal price,
            BigDecimal quantity,
            BigDecimal cashAfter,
            BigDecimal equityAfter,
            BigDecimal pnlPct
    ) {
    }

    public record EquityRow(
            LocalDate tradeDate,
            BigDecimal equity,
            BigDecimal drawdownPct
    ) {
    }
}
