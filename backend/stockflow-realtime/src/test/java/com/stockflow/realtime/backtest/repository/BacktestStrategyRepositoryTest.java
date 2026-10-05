package com.stockflow.realtime.backtest.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BacktestStrategyRepositoryTest {

    private JdbcTemplate jdbc;
    private BacktestStrategyRepository repository;

    private static final Map<String, Object> ROW = Map.of(
            "id", 1L, "name", "n", "symbol", "BTCUSDT", "strategy_type", "RSI",
            "params", "{\"period\":14}", "initial_cash", "10000",
            "created_at", Instant.EPOCH, "updated_at", Instant.EPOCH);

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        repository = new BacktestStrategyRepository(jdbc, new ObjectMapper());
    }

    @SuppressWarnings("unchecked")
    private void stubQueryReturnsRows(List<Map<String, Object>> rows) {
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), rows));
        when(jdbc.query(anyString(), any(RowMapper.class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), rows));
    }

    @Test
    void insertWritesJsonParamsThenReloadsRow() {
        when(jdbc.queryForObject(anyString(), eq(Long.class), any(Object[].class))).thenReturn(1L);
        stubQueryReturnsRows(List.of(ROW));

        StrategyRow row = repository.insert("n", "btcusdt", StrategyType.RSI, Map.of("period", 14), BigDecimal.TEN);

        assertThat(row.id()).isEqualTo(1L);
        assertThat(row.params()).containsEntry("period", 14);
        assertThat(row.createdAt()).isEqualTo(Instant.EPOCH);
        ArgumentCaptor<Object> args = ArgumentCaptor.forClass(Object.class);
        verify(jdbc).queryForObject(anyString(), eq(Long.class), args.capture(), args.capture(), args.capture(),
                args.capture(), args.capture());
        assertThat(args.getAllValues()).contains("BTCUSDT", "RSI", "{\"period\":14}");
    }

    @Test
    void findAllFiltersBySymbolOnlyWhenGiven() {
        stubQueryReturnsRows(List.of(ROW, ROW));

        assertThat(repository.findAll("btcusdt")).hasSize(2);
        assertThat(repository.findAll(null)).hasSize(2);
        assertThat(repository.findAll("  ")).hasSize(2);
        verify(jdbc).query(eq("SELECT * FROM backtest_strategies WHERE symbol = ? ORDER BY id DESC"),
                any(RowMapper.class), eq("BTCUSDT"));
    }

    @Test
    void findByIdReturnsEmptyWhenMissing() {
        stubQueryReturnsRows(List.of());
        assertThat(repository.findById(5L)).isEmpty();
    }

    @Test
    void emptyOrBlankParamsMapToEmptyMap() {
        stubQueryReturnsRows(List.of(
                Map.of("id", 1L, "name", "n", "symbol", "S", "strategy_type", "RSI", "params", " ",
                        "initial_cash", "1", "created_at", Instant.EPOCH, "updated_at", Instant.EPOCH)));

        assertThat(repository.findById(1L).orElseThrow().params()).isEmpty();
    }

    @Test
    void malformedParamsJsonIsReportedAsIllegalState() {
        stubQueryReturnsRows(List.of(
                Map.of("id", 1L, "name", "n", "symbol", "S", "strategy_type", "RSI", "params", "{oops",
                        "initial_cash", "1", "created_at", Instant.EPOCH, "updated_at", Instant.EPOCH)));

        assertThatThrownBy(() -> repository.findById(1L)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void updateReloadsOnlyWhenARowChanged() {
        stubQueryReturnsRows(List.of(ROW));
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        assertThat(repository.update(1L, "n", "btc", StrategyType.RSI, null, BigDecimal.ONE)).isPresent();

        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(0);
        assertThat(repository.update(1L, "n", "btc", StrategyType.RSI, null, BigDecimal.ONE))
                .isEqualTo(Optional.empty());
    }

    @Test
    void deleteReportsWhetherARowWasRemoved() {
        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1, 0);

        assertThat(repository.delete(1L)).isTrue();
        assertThat(repository.delete(1L)).isFalse();
    }
}
