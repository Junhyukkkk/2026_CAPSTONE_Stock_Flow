package com.stockflow.realtime.stock;

import com.stockflow.realtime.stock.dto.IndicatorResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
@RequiredArgsConstructor
public class IndicatorHistoryService {

    private final JdbcTemplate jdbcTemplate;

    public List<IndicatorResponse> getIndicators(String symbol, LocalDate from, LocalDate to) {
        return jdbcTemplate.query(
                """
                SELECT symbol, trade_date, ma5, ma20, ma60, rsi14, macd, macd_signal, macd_hist
                FROM symbol_daily_indicators
                WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date ASC
                """,
                (rs, rowNum) -> IndicatorResponse.builder()
                        .symbol(rs.getString("symbol"))
                        .date(rs.getDate("trade_date").toLocalDate())
                        .ma5(rs.getBigDecimal("ma5"))
                        .ma20(rs.getBigDecimal("ma20"))
                        .ma60(rs.getBigDecimal("ma60"))
                        .rsi14(rs.getBigDecimal("rsi14"))
                        .macd(rs.getBigDecimal("macd"))
                        .macdSignal(rs.getBigDecimal("macd_signal"))
                        .macdHist(rs.getBigDecimal("macd_hist"))
                        .build(),
                symbol.toUpperCase(), from, to
        );
    }
}
