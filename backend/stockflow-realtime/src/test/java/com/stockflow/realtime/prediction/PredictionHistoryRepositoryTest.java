package com.stockflow.realtime.prediction;

import com.stockflow.realtime.prediction.PredictionHistoryRepository.PointRecord;
import com.stockflow.realtime.prediction.PredictionHistoryRepository.RunRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PredictionHistoryRepositoryTest {

    private JdbcTemplate jdbcTemplate;
    private PredictionHistoryRepository repository;

    private final RunRecord run = new RunRecord(
            "BTCUSDT", "1m", 3, "", Instant.parse("2026-10-05T03:07:00Z"), 1.0, 10, "[]");
    private final List<PointRecord> points = List.of(
            new PointRecord("A", Instant.parse("2026-10-05T03:08:00Z"), 1.0, null, null, null, null, null));

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        TransactionTemplate tx = mock(TransactionTemplate.class);
        when(tx.execute(any())).thenAnswer(invocation ->
                invocation.<TransactionCallback<Object>>getArgument(0).doInTransaction(null));
        repository = new PredictionHistoryRepository(jdbcTemplate, tx);
    }

    @SuppressWarnings("unchecked")
    private void stubRunInsert(List<Long> returned) {
        when(jdbcTemplate.query(contains("INSERT INTO prediction_runs"), any(RowMapper.class), any(Object[].class)))
                .thenReturn(returned);
    }

    @Test
    @SuppressWarnings("unchecked")
    void 이미_있는_run이면_points를_넣지_않는다() {
        stubRunInsert(List.of());

        boolean saved = repository.save(run, points);

        assertThat(saved).isFalse();
        verify(jdbcTemplate, never()).batchUpdate(anyString(), any(Collection.class), anyInt(),
                any(ParameterizedPreparedStatementSetter.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void 새_run이면_같은_트랜잭션에서_points를_배치로_넣는다() {
        stubRunInsert(List.of(7L));

        boolean saved = repository.save(run, points);

        assertThat(saved).isTrue();
        verify(jdbcTemplate).batchUpdate(contains("INSERT INTO prediction_forecast_points"),
                any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void run_삽입_SQL은_ON_CONFLICT_DO_NOTHING을_쓴다() {
        stubRunInsert(List.of(7L));
        org.mockito.ArgumentCaptor<String> sql = org.mockito.ArgumentCaptor.forClass(String.class);

        repository.save(run, List.of());

        verify(jdbcTemplate).query(sql.capture(), any(RowMapper.class), any(Object[].class));
        assertThat(sql.getValue()).contains("ON CONFLICT (symbol, \"interval\", horizon, source, base_ts) DO NOTHING");
        verify(jdbcTemplate, never()).batchUpdate(anyString(), any(Collection.class), anyInt(),
                any(ParameterizedPreparedStatementSetter.class));
    }
}
