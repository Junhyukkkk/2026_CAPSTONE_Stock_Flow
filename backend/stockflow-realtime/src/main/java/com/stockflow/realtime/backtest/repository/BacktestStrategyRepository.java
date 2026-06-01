package com.stockflow.realtime.backtest.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.backtest.model.StrategyType;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * backtest_strategies CRUD. params 는 JSONB 컬럼으로 저장한다.
 */
@Repository
@RequiredArgsConstructor
public class BacktestStrategyRepository {

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private final RowMapper<StrategyRow> rowMapper = (rs, rowNum) -> new StrategyRow(
            rs.getLong("id"),
            rs.getString("name"),
            rs.getString("symbol"),
            rs.getString("strategy_type"),
            readParams(rs.getString("params")),
            rs.getBigDecimal("initial_cash"),
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant()
    );

    public StrategyRow insert(String name, String symbol, StrategyType type,
                              Map<String, Object> params, BigDecimal initialCash) {
        Long id = jdbcTemplate.queryForObject(
                """
                INSERT INTO backtest_strategies (name, symbol, strategy_type, params, initial_cash)
                VALUES (?, ?, ?, CAST(? AS jsonb), ?)
                RETURNING id
                """,
                Long.class,
                name, symbol.toUpperCase(), type.name(), writeParams(params), initialCash);
        return findById(id).orElseThrow();
    }

    public List<StrategyRow> findAll(String symbol) {
        if (symbol != null && !symbol.isBlank()) {
            return jdbcTemplate.query(
                    "SELECT * FROM backtest_strategies WHERE symbol = ? ORDER BY id DESC",
                    rowMapper, symbol.toUpperCase());
        }
        return jdbcTemplate.query(
                "SELECT * FROM backtest_strategies ORDER BY id DESC", rowMapper);
    }

    public Optional<StrategyRow> findById(long id) {
        List<StrategyRow> rows = jdbcTemplate.query(
                "SELECT * FROM backtest_strategies WHERE id = ?", rowMapper, id);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public Optional<StrategyRow> update(long id, String name, String symbol, StrategyType type,
                                        Map<String, Object> params, BigDecimal initialCash) {
        int updated = jdbcTemplate.update(
                """
                UPDATE backtest_strategies
                   SET name = ?, symbol = ?, strategy_type = ?, params = CAST(? AS jsonb), initial_cash = ?
                 WHERE id = ?
                """,
                name, symbol.toUpperCase(), type.name(), writeParams(params), initialCash, id);
        return updated == 0 ? Optional.empty() : findById(id);
    }

    public boolean delete(long id) {
        return jdbcTemplate.update("DELETE FROM backtest_strategies WHERE id = ?", id) > 0;
    }

    private Map<String, Object> readParams(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to parse strategy params JSON", e);
        }
    }

    private String writeParams(Map<String, Object> params) {
        try {
            return objectMapper.writeValueAsString(params == null ? Collections.emptyMap() : params);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize strategy params", e);
        }
    }

    public record StrategyRow(
            long id,
            String name,
            String symbol,
            String strategyType,
            Map<String, Object> params,
            BigDecimal initialCash,
            Instant createdAt,
            Instant updatedAt
    ) {
    }
}
