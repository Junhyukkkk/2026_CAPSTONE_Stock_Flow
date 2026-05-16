package com.stockflow.realtime.stock;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class InstrumentRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final RowMapper<InstrumentRow> ROW_MAPPER = (rs, rowNum) -> new InstrumentRow(
            rs.getString("symbol"),
            rs.getString("name"),
            rs.getString("market_type"),
            rs.getString("exchange"),
            rs.getBoolean("is_active"),
            rs.getTimestamp("last_seen_at").toInstant()
    );

    public List<InstrumentRow> findAll(String marketType, boolean activeOnly) {
        List<Object> params = new ArrayList<>();
        StringBuilder sql = new StringBuilder(
                "SELECT symbol, name, market_type, exchange, is_active, last_seen_at FROM instruments WHERE 1=1 ");

        if (marketType != null) {
            sql.append("AND market_type = ? ");
            params.add(marketType.toUpperCase());
        }
        if (activeOnly) {
            sql.append("AND is_active = true ");
        }
        sql.append("ORDER BY last_seen_at DESC");

        return jdbcTemplate.query(sql.toString(), ROW_MAPPER, params.toArray());
    }

    public Optional<InstrumentRow> findBySymbol(String symbol) {
        List<InstrumentRow> rows = jdbcTemplate.query(
                "SELECT symbol, name, market_type, exchange, is_active, last_seen_at FROM instruments WHERE symbol = ?",
                ROW_MAPPER,
                symbol.toUpperCase()
        );
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    public void upsert(String symbol, String marketType, String exchange, String name) {
        jdbcTemplate.update(
                "SELECT register_instrument(?, ?, ?, ?)",
                symbol.toUpperCase(),
                marketType.toUpperCase(),
                exchange.toUpperCase(),
                name != null ? name : symbol.toUpperCase()
        );
    }

    public int update(String symbol, String name, String exchange) {
        List<Object> params = new ArrayList<>();
        List<String> setClauses = new ArrayList<>();

        if (name != null && !name.isBlank()) {
            setClauses.add("name = ?");
            params.add(name.trim());
        }
        if (exchange != null && !exchange.isBlank()) {
            setClauses.add("exchange = ?");
            params.add(exchange.trim().toUpperCase());
        }
        if (setClauses.isEmpty()) {
            return 0;
        }

        params.add(symbol.toUpperCase());
        String sql = "UPDATE instruments SET " + String.join(", ", setClauses) + " WHERE symbol = ?";
        return jdbcTemplate.update(sql, params.toArray());
    }

    public int setActive(String symbol, boolean active) {
        return jdbcTemplate.update(
                "UPDATE instruments SET is_active = ? WHERE symbol = ?",
                active, symbol.toUpperCase()
        );
    }

    public record InstrumentRow(
            String symbol,
            String name,
            String marketType,
            String exchange,
            boolean active,
            Instant lastSeenAt
    ) {}
}
