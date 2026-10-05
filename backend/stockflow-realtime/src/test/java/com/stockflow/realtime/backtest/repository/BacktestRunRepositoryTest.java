package com.stockflow.realtime.backtest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.backtest.engine.BacktestResult;
import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.DataCoverage;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.RunRow;
import com.stockflow.realtime.prediction.PredictionSignalResponse.PredictionSignalPoint;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class BacktestRunRepositoryTest {

    private static final LocalDate D1 = LocalDate.of(2025, 1, 1);
    private static final LocalDate D2 = LocalDate.of(2025, 1, 2);

    private JdbcTemplate jdbc;
    private BacktestRunRepository repository;
    private final List<PreparedStatement> statements = new ArrayList<>();

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        repository = new BacktestRunRepository(jdbc, new ObjectMapper());
        // batchUpdate: 각 원소에 대해 setter 를 실행해 PreparedStatement 바인딩 코드까지 통과시킨다.
        when(jdbc.batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class)))
                .thenAnswer(inv -> {
                    Collection<Object> items = inv.getArgument(1);
                    ParameterizedPreparedStatementSetter<Object> setter = inv.getArgument(3);
                    for (Object item : items) {
                        PreparedStatement ps = mock(PreparedStatement.class);
                        statements.add(ps);
                        setter.setValues(ps, item);
                    }
                    return new int[][] {new int[items.size()]};
                });
    }

    private void stubRows(List<Map<String, Object>> rows) {
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), rows));
    }

    private static Map<String, Object> runRow() {
        Map<String, Object> row = new java.util.HashMap<>();
        row.put("id", 7L);
        row.put("strategy_id", 3L);
        row.put("symbol", "BTCUSDT");
        row.put("strategy_type", "PREDICTION");
        row.put("params", "{\"model\":\"ARIMA\"}");
        row.put("from_date", D1);
        row.put("to_date", D2);
        row.put("initial_cash", "10000");
        row.put("final_equity", "11000");
        row.put("total_return_pct", "10");
        row.put("cagr_pct", "5");
        row.put("mdd_pct", "2");
        row.put("trade_count", 4);
        row.put("win_rate_pct", "50");
        row.put("bar_count", 30);
        row.put("status", "SUCCESS");
        row.put("created_at", Instant.EPOCH);
        return row;
    }

    @Test
    void loadBarsMapsRowsAndUppercasesSymbol() {
        stubRows(List.of(Map.of("trade_date", D1, "open", "1", "high", "2", "low", "0.5", "close", "1.5", "volume", "9")));

        List<Bar> bars = repository.loadBars("btcusdt", "BINANCE", D1, D2);

        assertThat(bars).containsExactly(new Bar(D1, new BigDecimal("1"), new BigDecimal("2"),
                new BigDecimal("0.5"), new BigDecimal("1.5"), new BigDecimal("9")));
        verify(jdbc).query(contains("DISTINCT ON"), any(RowMapper.class), eq("BTCUSDT"), eq("BINANCE"), any(), any());
    }

    @Test
    void countsHandleNullFromDatabase() {
        when(jdbc.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenReturn(null, 12, null, 5);

        assertThat(repository.countBarsBefore("btc", "BINANCE", D1)).isZero();
        assertThat(repository.countBarsBefore("btc", "BINANCE", D1)).isEqualTo(12);
        assertThat(repository.countKnownCryptoSymbols("BINANCE")).isZero();
        assertThat(repository.countKnownCryptoSymbols("BINANCE")).isEqualTo(5);
    }

    @Test
    void findCoverageReturnsRangeOrEmptyCoverage() {
        when(jdbc.query(anyString(), any(ResultSetExtractor.class), any(Object[].class)))
                .thenAnswer(inv -> inv.getArgument(1, ResultSetExtractor.class).extractData(
                        JdbcTestSupport.resultSet(Map.of("first_date", D1, "last_date", D2, "bar_count", 2))))
                .thenAnswer(inv -> inv.getArgument(1, ResultSetExtractor.class).extractData(
                        JdbcTestSupport.resultSet(Map.of("bar_count", 0))))
                .thenAnswer(inv -> inv.getArgument(1, ResultSetExtractor.class).extractData(
                        JdbcTestSupport.emptyResultSet()));

        assertThat(repository.findCoverage("btc", "BINANCE")).isEqualTo(new DataCoverage(D1, D2, 2));
        assertThat(repository.findCoverage("btc", "BINANCE")).isEqualTo(new DataCoverage(null, null, 0));
        assertThat(repository.findCoverage("btc", "BINANCE")).isEqualTo(new DataCoverage(null, null, 0));
    }

    @Test
    void findEligibleCryptoSymbolsPassesExpectedBarCount() {
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of("BTCUSDT"));

        assertThat(repository.findEligibleCryptoSymbols("BINANCE", D1, D2, 50)).containsExactly("BTCUSDT");
        verify(jdbc).queryForList(contains("HAVING"), eq(String.class), eq("BINANCE"), any(), any(), eq(2), any(), eq(50));
    }

    private BacktestResult result(boolean withPoints) {
        return new BacktestResult(BigDecimal.valueOf(100), BigDecimal.valueOf(110), BigDecimal.TEN, null,
                BigDecimal.ONE, 1, null, 2,
                withPoints ? List.of(new BacktestResult.Trade(1, D1, BacktestResult.Side.BUY, BigDecimal.ONE,
                        BigDecimal.TEN, BigDecimal.ZERO, BigDecimal.TEN, null)) : List.of(),
                withPoints ? List.of(new BacktestResult.EquityPoint(D1, BigDecimal.TEN, BigDecimal.ZERO)) : List.of());
    }

    @Test
    void saveResultInsertsHeaderTradesAndEquityCurve() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(42L);

        long id = repository.saveResult(null, "btc", "BUY_AND_HOLD", null, D1, D2, result(true));

        assertThat(id).isEqualTo(42L);
        assertThat(statements).hasSize(2); // trade 1건 + equity point 1건
        verify(jdbc).batchUpdate(contains("backtest_trades"), any(Collection.class), eq(1), any(ParameterizedPreparedStatementSetter.class));
        verify(jdbc).batchUpdate(contains("backtest_equity_curve"), any(Collection.class), eq(1), any(ParameterizedPreparedStatementSetter.class));
    }

    @Test
    void saveResultSkipsBatchesWhenNothingToInsert() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(1L);

        repository.saveResult(1L, "btc", "RSI", Map.of("period", 14), D1, D2, result(false));

        verify(jdbc, never()).batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));
    }

    @Test
    void saveFailureTruncatesLongErrors() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(9L);

        assertThat(repository.saveFailure(null, "btc", "RSI", Map.of(), D1, D2, BigDecimal.TEN, "x".repeat(5000)))
                .isEqualTo(9L);
        assertThat(repository.saveFailure(null, "btc", "RSI", null, D1, D2, BigDecimal.TEN, null)).isEqualTo(9L);
        verify(jdbc).queryForObject(contains("'FAILED'"), eq(Long.class), any(), any(), any(), any(), any(), any(),
                any(), eq("x".repeat(4000)));
    }

    @Test
    void savePredictionPointsBindsEveryPoint() {
        repository.savePredictionPoints(1L, null);
        repository.savePredictionPoints(1L, List.of());
        verify(jdbc, never()).batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));

        repository.savePredictionPoints(1L, List.of(
                new PredictionSignalPoint(D1, D2, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.ONE, "BUY")));

        assertThat(statements).hasSize(1);
    }

    @Test
    void findRunMapsAllColumnsAndNullableOnes() {
        Map<String, Object> sparse = new java.util.HashMap<>(runRow());
        sparse.put("strategy_id", null);
        sparse.put("trade_count", null);
        sparse.put("bar_count", null);
        sparse.put("params", null);
        stubRows(List.of(runRow()));
        RunRow full = repository.findRun(7L).orElseThrow();
        assertThat(full.strategyId()).isEqualTo(3L);
        assertThat(full.params()).containsEntry("model", "ARIMA");
        assertThat(full.tradeCount()).isEqualTo(4);
        assertThat(full.createdAt()).isEqualTo(Instant.EPOCH);

        stubRows(List.of(sparse));
        RunRow empty = repository.findRun(7L).orElseThrow();
        assertThat(empty.strategyId()).isNull();
        assertThat(empty.tradeCount()).isNull();
        assertThat(empty.barCount()).isNull();
        assertThat(empty.params()).isEmpty();

        stubRows(List.of());
        assertThat(repository.findRun(7L)).isEmpty();
    }

    @Test
    void findRunsByStrategyReturnsAllRows() {
        stubRows(List.of(runRow(), runRow()));
        assertThat(repository.findRunsByStrategy(3L)).hasSize(2);
    }

    @Test
    void malformedStoredParamsFailLoudly() {
        Map<String, Object> bad = new java.util.HashMap<>(runRow());
        bad.put("params", "{nope");
        stubRows(List.of(bad));

        assertThatThrownBy(() -> repository.findRun(1L)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void runExistsChecksCount() {
        when(jdbc.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenReturn(1, 0, null);

        assertThat(repository.runExists(1L)).isTrue();
        assertThat(repository.runExists(1L)).isFalse();
        assertThat(repository.runExists(1L)).isFalse();
    }

    @Test
    void tradesEquityAndPredictionPointsAreMapped() {
        stubRows(List.of(Map.of("seq", 1, "trade_date", D1, "side", "BUY", "price", "1", "quantity", "2",
                "cash_after", "3", "equity_after", "4", "pnl_pct", "5")));
        assertThat(repository.findTrades(1L)).singleElement().satisfies(t -> {
            assertThat(t.side()).isEqualTo("BUY");
            assertThat(t.pnlPct()).isEqualByComparingTo("5");
        });

        stubRows(List.of(Map.of("trade_date", D1, "equity", "10", "drawdown_pct", "1")));
        assertThat(repository.findEquityCurve(1L)).singleElement()
                .satisfies(e -> assertThat(e.equity()).isEqualByComparingTo("10"));

        stubRows(List.of(Map.of("signal_date", D1, "execution_date", D2, "reference_price", "1",
                "predicted_price", "2", "expected_return_pct", "3", "threshold_pct", "4", "signal", "SELL")));
        assertThat(repository.findPredictionPoints(1L)).singleElement()
                .satisfies(p -> assertThat(p.signal()).isEqualTo("SELL"));
    }
}
