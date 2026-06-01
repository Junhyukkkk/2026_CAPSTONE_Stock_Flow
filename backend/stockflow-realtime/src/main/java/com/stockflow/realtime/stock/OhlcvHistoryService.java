package com.stockflow.realtime.stock;

import com.stockflow.realtime.stock.dto.OhlcvResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OhlcvHistoryService {

    private final JdbcTemplate jdbcTemplate;

    public List<OhlcvResponse> getOhlcv(String symbol, LocalDate from, LocalDate to) {
        return jdbcTemplate.query(
                """
                SELECT symbol, trade_date, open, high, low, close, volume, tick_count
                FROM symbol_daily_ohlcv
                WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date ASC
                """,
                (rs, rowNum) -> OhlcvResponse.builder()
                        .symbol(rs.getString("symbol"))
                        .date(rs.getDate("trade_date").toLocalDate())
                        .open(rs.getBigDecimal("open"))
                        .high(rs.getBigDecimal("high"))
                        .low(rs.getBigDecimal("low"))
                        .close(rs.getBigDecimal("close"))
                        .volume(rs.getBigDecimal("volume"))
                        .tickCount(rs.getLong("tick_count"))
                        .build(),
                symbol.toUpperCase(), from, to
        );
    }
}
