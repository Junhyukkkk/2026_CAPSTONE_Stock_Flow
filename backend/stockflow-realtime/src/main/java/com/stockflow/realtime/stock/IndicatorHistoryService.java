package com.stockflow.realtime.stock;

import com.stockflow.realtime.stock.dto.IndicatorResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class IndicatorHistoryService {

    private final JdbcTemplate jdbcTemplate;

    private static final String SELECT_COLUMNS = """
            SELECT symbol, trade_date, ma5, ma20, ma60, rsi14,
                   macd, macd_signal, macd_hist,
                   bb_upper, bb_lower, stoch_k, stoch_d, atr14, obv
            FROM symbol_daily_indicators
            """;

    private final RowMapper<IndicatorResponse> rowMapper = (rs, rowNum) -> mapRow(rs);

    private IndicatorResponse mapRow(ResultSet rs) throws SQLException {
        return IndicatorResponse.builder()
                .symbol(rs.getString("symbol"))
                .tradeDate(rs.getDate("trade_date").toLocalDate())
                .ma5(rs.getBigDecimal("ma5"))
                .ma20(rs.getBigDecimal("ma20"))
                .ma60(rs.getBigDecimal("ma60"))
                .rsi14(rs.getBigDecimal("rsi14"))
                .macd(rs.getBigDecimal("macd"))
                .macdSignal(rs.getBigDecimal("macd_signal"))
                .macdHist(rs.getBigDecimal("macd_hist"))
                .bbUpper(rs.getBigDecimal("bb_upper"))
                .bbMiddle(rs.getBigDecimal("ma20"))
                .bbLower(rs.getBigDecimal("bb_lower"))
                .stochK(rs.getBigDecimal("stoch_k"))
                .stochD(rs.getBigDecimal("stoch_d"))
                .atr14(rs.getBigDecimal("atr14"))
                .obv(rs.getObject("obv") != null ? rs.getLong("obv") : null)
                .build();
    }

    /**
     * 기간별 지표 조회
     */
    public List<IndicatorResponse> getIndicators(String symbol, LocalDate from, LocalDate to) {
        String sql = SELECT_COLUMNS + """
                WHERE symbol = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date ASC
                """;
        return jdbcTemplate.query(sql, rowMapper,
                symbol.toUpperCase(), Date.valueOf(from), Date.valueOf(to));
    }

    /**
     * 최신 지표 조회
     */
    public Optional<IndicatorResponse> getLatest(String symbol) {
        String sql = SELECT_COLUMNS + """
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT 1
                """;
        List<IndicatorResponse> results = jdbcTemplate.query(sql, rowMapper, symbol.toUpperCase());
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * 최근 N일 지표 히스토리 조회
     */
    public List<IndicatorResponse> getHistory(String symbol, int days) {
        String sql = SELECT_COLUMNS + """
                WHERE symbol = ?
                ORDER BY trade_date DESC
                LIMIT ?
                """;
        return jdbcTemplate.query(sql, rowMapper, symbol.toUpperCase(), days);
    }
}
