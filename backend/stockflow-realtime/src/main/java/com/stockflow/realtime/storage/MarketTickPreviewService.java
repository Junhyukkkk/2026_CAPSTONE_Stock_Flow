package com.stockflow.realtime.storage;

import com.stockflow.realtime.storage.dto.StorageOverviewResponse;
import com.stockflow.realtime.storage.dto.StorageOverviewResponse.MarketTickOneMinuteRow;
import com.stockflow.realtime.storage.dto.StorageOverviewResponse.MarketTickPreviewRow;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * 읽기 전용 미리보기. 데모·발표용으로 최근 행만 소량 조회한다.
 */
@Service
@RequiredArgsConstructor
public class MarketTickPreviewService {

    private static final int MAX_LIMIT = 100;
    private static final int DEFAULT_LIMIT = 25;

    private final JdbcTemplate jdbcTemplate;

    public StorageOverviewResponse overview(Integer limit) {
        int n = limit == null || limit < 1 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);

        try {
            Long approx = jdbcTemplate.queryForObject(
                    """
                            SELECT COALESCE(s.n_live_tup::bigint, c.reltuples::bigint, 0::bigint)
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                            LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
                            WHERE n.nspname = 'public'
                              AND c.relname = 'market_ticks'
                              AND c.relkind = 'r'
                            """,
                    Long.class);
            long approximateTickRows = approx != null ? approx : 0L;

            List<MarketTickPreviewRow> recentTicks = jdbcTemplate.query(
                    """
                            SELECT id, source, symbol, trade_id, price, volume, ts, ingested_at
                            FROM market_ticks
                            ORDER BY ts DESC
                            LIMIT ?
                            """,
                    (rs, rowNum) -> new MarketTickPreviewRow(
                            rs.getLong("id"),
                            rs.getString("source"),
                            rs.getString("symbol"),
                            rs.getString("trade_id"),
                            rs.getBigDecimal("price"),
                            rs.getBigDecimal("volume"),
                            toInstant(rs.getTimestamp("ts")),
                            toInstant(rs.getTimestamp("ingested_at"))
                    ),
                    n);

            boolean hasOneMinute = Boolean.TRUE.equals(jdbcTemplate.queryForObject(
                    "SELECT to_regclass('public.market_ticks_1m') IS NOT NULL",
                    Boolean.class));

            List<MarketTickOneMinuteRow> bars = Collections.emptyList();
            if (hasOneMinute) {
                bars = jdbcTemplate.query(
                        """
                                SELECT bucket, symbol, source, open, high, low, close, volume
                                FROM market_ticks_1m
                                ORDER BY bucket DESC
                                LIMIT ?
                                """,
                        (rs, rowNum) -> new MarketTickOneMinuteRow(
                                toInstant(rs.getTimestamp("bucket")),
                                rs.getString("symbol"),
                                rs.getString("source"),
                                rs.getBigDecimal("open"),
                                rs.getBigDecimal("high"),
                                rs.getBigDecimal("low"),
                                rs.getBigDecimal("close"),
                                rs.getBigDecimal("volume")
                        ),
                        Math.min(n, 30));
            }

            return new StorageOverviewResponse(
                    approximateTickRows,
                    hasOneMinute,
                    recentTicks,
                    bars);
        } catch (DataAccessException e) {
            return new StorageOverviewResponse(
                    0L,
                    false,
                    Collections.emptyList(),
                    Collections.emptyList());
        }
    }

    private static Instant toInstant(Timestamp ts) {
        return ts == null ? null : ts.toInstant();
    }
}
