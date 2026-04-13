package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

/**
 * Storage consumer 전용: 정규화 틱을 TimescaleDB(PostgreSQL)에 배치 삽입.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MarketTickBulkWriter {

    private static final String INSERT_SQL = """
            INSERT INTO market_ticks (
                source, symbol, trade_id, price, volume, exchange, ts, received_at, market_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (symbol, source, trade_id, ts) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    public void insertBatch(List<NormalizedTradeDTO> trades) {
        if (trades == null || trades.isEmpty()) {
            return;
        }

        jdbcTemplate.batchUpdate(INSERT_SQL, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                NormalizedTradeDTO t = trades.get(i);
                ps.setString(1, t.getSource());
                ps.setString(2, t.getSymbol());
                ps.setString(3, t.getTradeId());
                ps.setBigDecimal(4, t.getPrice());
                ps.setBigDecimal(5, t.getVolume());
                ps.setString(6, t.getExchange());
                ps.setTimestamp(7, Timestamp.from(Instant.ofEpochMilli(t.getTimestamp())));
                ps.setTimestamp(8, Timestamp.from(Instant.ofEpochMilli(t.getReceivedAt())));
                ps.setString(9, t.getMarketType());
            }

            @Override
            public int getBatchSize() {
                return trades.size();
            }
        });

        log.debug("Inserted market_ticks batch: size={}", trades.size());
    }
}
