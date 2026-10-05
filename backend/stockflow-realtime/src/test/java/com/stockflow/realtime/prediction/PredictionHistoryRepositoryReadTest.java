package com.stockflow.realtime.prediction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** 조회 SQL 은 표준 SQL 이라 H2 로 조립 로직(정렬·limit·그룹핑·쿼리 횟수)을 검증한다. */
class PredictionHistoryRepositoryReadTest {

    private JdbcTemplate jdbc;
    private PredictionHistoryRepository repository;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource ds = new DriverManagerDataSource(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1", "sa", "");
        jdbc = spy(new JdbcTemplate(ds));
        jdbc.execute("""
                CREATE TABLE prediction_runs (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(32) NOT NULL,
                    "interval" VARCHAR(8) NOT NULL,
                    horizon INT NOT NULL,
                    source VARCHAR(32) NOT NULL DEFAULT '',
                    base_ts TIMESTAMP WITH TIME ZONE NOT NULL,
                    base_value DOUBLE PRECISION,
                    requested_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    latency_ms INT
                )""");
        jdbc.execute("""
                CREATE TABLE prediction_forecast_points (
                    run_id BIGINT NOT NULL,
                    model VARCHAR(64) NOT NULL,
                    ts TIMESTAMP WITH TIME ZONE NOT NULL,
                    yhat DOUBLE PRECISION NOT NULL,
                    yhat_lower DOUBLE PRECISION,
                    yhat_upper DOUBLE PRECISION,
                    trained_at TIMESTAMP WITH TIME ZONE,
                    mae DOUBLE PRECISION,
                    rmse DOUBLE PRECISION,
                    PRIMARY KEY (run_id, model, ts)
                )""");
        repository = new PredictionHistoryRepository(jdbc, mock(TransactionTemplate.class));
    }

    private long insertRun(String symbol, String interval, String baseTs, Double baseValue, Integer latencyMs) {
        jdbc.update("""
                INSERT INTO prediction_runs (symbol, "interval", horizon, source, base_ts, base_value, latency_ms)
                VALUES (?, ?, 3, '', ?, ?, ?)
                """, symbol, interval, java.sql.Timestamp.from(Instant.parse(baseTs)), baseValue, latencyMs);
        return jdbc.queryForObject("SELECT MAX(id) FROM prediction_runs", Long.class);
    }

    private void insertPoint(long runId, String model, String ts, double yhat,
                             Double lower, Double upper, String trainedAt, Double mae, Double rmse) {
        jdbc.update("""
                INSERT INTO prediction_forecast_points
                    (run_id, model, ts, yhat, yhat_lower, yhat_upper, trained_at, mae, rmse)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, runId, model, java.sql.Timestamp.from(Instant.parse(ts)), yhat, lower, upper,
                trainedAt == null ? null : java.sql.Timestamp.from(Instant.parse(trainedAt)), mae, rmse);
    }

    @SuppressWarnings("unchecked")
    private void verifyQueryCount(int expected) {
        verify(jdbc, times(expected)).query(anyString(), any(RowMapper.class), any(Object[].class));
    }

    @Test
    void run은_base_ts_내림차순으로_limit개만_돌려준다() {
        insertRun("BTCUSDT", "1m", "2026-10-05T03:05:00Z", 1.0, 10);
        insertRun("BTCUSDT", "1m", "2026-10-05T03:07:00Z", 3.0, 30);
        insertRun("BTCUSDT", "1m", "2026-10-05T03:06:00Z", 2.0, 20);

        List<PredictionHistoryResponse> all = repository.findRecent("BTCUSDT", "1m", 20);
        List<PredictionHistoryResponse> top2 = repository.findRecent("BTCUSDT", "1m", 2);

        assertThat(all).extracting(PredictionHistoryResponse::baseTs).containsExactly(
                Instant.parse("2026-10-05T03:07:00Z"),
                Instant.parse("2026-10-05T03:06:00Z"),
                Instant.parse("2026-10-05T03:05:00Z"));
        assertThat(top2).extracting(PredictionHistoryResponse::baseValue).containsExactly(3.0, 2.0);
    }

    @Test
    void 다른_심볼_interval의_run은_제외한다() {
        insertRun("BTCUSDT", "1m", "2026-10-05T03:07:00Z", 1.0, 10);
        insertRun("ETHUSDT", "1m", "2026-10-05T03:07:00Z", 2.0, 10);
        insertRun("BTCUSDT", "1d", "2026-10-05T00:00:00Z", 3.0, 10);

        List<PredictionHistoryResponse> result = repository.findRecent("BTCUSDT", "1m", 20);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).symbol()).isEqualTo("BTCUSDT");
        assertThat(result.get(0).interval()).isEqualTo("1m");
        assertThat(result.get(0).horizon()).isEqualTo(3);
        assertThat(result.get(0).source()).isEmpty();
        assertThat(result.get(0).requestedAt()).isNotNull();
        assertThat(result.get(0).latencyMs()).isEqualTo(10);
    }

    @Test
    void run_모델_points로_그룹핑하고_points는_ts_오름차순이다() {
        long newer = insertRun("BTCUSDT", "1m", "2026-10-05T03:07:00Z", 100.0, 5);
        long older = insertRun("BTCUSDT", "1m", "2026-10-05T03:06:00Z", 90.0, 6);
        // 일부러 ts 역순으로 삽입
        insertPoint(newer, "ARIMA", "2026-10-05T03:10:00Z", 3.0, null, null, "2026-10-05T03:11:00Z", null, null);
        insertPoint(newer, "ARIMA", "2026-10-05T03:08:00Z", 1.0, 0.5, 1.5, "2026-10-05T03:11:00Z", null, null);
        insertPoint(newer, "ARIMA", "2026-10-05T03:09:00Z", 2.0, null, null, "2026-10-05T03:11:00Z", null, null);
        insertPoint(newer, "Chronos-Bolt", "2026-10-05T03:09:00Z", 20.0, 19.0, 21.0, null, 7.5, 9.5);
        insertPoint(newer, "Chronos-Bolt", "2026-10-05T03:08:00Z", 10.0, 9.0, 11.0, null, 7.5, 9.5);
        insertPoint(older, "ARIMA", "2026-10-05T03:07:00Z", 5.0, null, null, null, null, null);

        List<PredictionHistoryResponse> result = repository.findRecent("BTCUSDT", "1m", 20);

        assertThat(result).extracting(PredictionHistoryResponse::runId).containsExactly(newer, older);
        PredictionHistoryResponse first = result.get(0);
        assertThat(first.models()).extracting(PredictionHistoryResponse.ModelForecast::model)
                .containsExactly("ARIMA", "Chronos-Bolt");

        PredictionHistoryResponse.ModelForecast arima = first.models().get(0);
        assertThat(arima.trainedAt()).isEqualTo(Instant.parse("2026-10-05T03:11:00Z"));
        assertThat(arima.mae()).isNull();
        assertThat(arima.rmse()).isNull();
        assertThat(arima.points()).extracting(PredictionHistoryResponse.Point::ts).containsExactly(
                Instant.parse("2026-10-05T03:08:00Z"),
                Instant.parse("2026-10-05T03:09:00Z"),
                Instant.parse("2026-10-05T03:10:00Z"));
        assertThat(arima.points().get(0).yhatLower()).isEqualTo(0.5);
        assertThat(arima.points().get(0).yhatUpper()).isEqualTo(1.5);
        assertThat(arima.points().get(1).yhatLower()).isNull();

        PredictionHistoryResponse.ModelForecast chronos = first.models().get(1);
        assertThat(chronos.trainedAt()).isNull();
        assertThat(chronos.mae()).isEqualTo(7.5);
        assertThat(chronos.rmse()).isEqualTo(9.5);
        assertThat(chronos.points()).extracting(PredictionHistoryResponse.Point::yhat).containsExactly(10.0, 20.0);

        assertThat(result.get(1).models()).hasSize(1);
        assertThat(result.get(1).models().get(0).points()).hasSize(1);
    }

    @Test
    void points가_없는_run은_빈_models를_가진다_그리고_null_허용_필드를_유지한다() {
        insertRun("BTCUSDT", "1m", "2026-10-05T03:07:00Z", null, null);

        List<PredictionHistoryResponse> result = repository.findRecent("BTCUSDT", "1m", 20);

        assertThat(result).hasSize(1);
        assertThat(result.get(0).baseValue()).isNull();
        assertThat(result.get(0).latencyMs()).isNull();
        assertThat(result.get(0).models()).isEmpty();
    }

    @Test
    void 이력이_없으면_빈_목록이고_points_쿼리는_실행하지_않는다() {
        List<PredictionHistoryResponse> result = repository.findRecent("BTCUSDT", "1m", 20);

        assertThat(result).isEmpty();
        verifyQueryCount(1);
    }

    @Test
    void run이_여러_개여도_쿼리는_2회뿐이다() {
        for (int i = 0; i < 5; i++) {
            long id = insertRun("BTCUSDT", "1m", "2026-10-05T03:0" + i + ":00Z", 1.0, 1);
            insertPoint(id, "ARIMA", "2026-10-05T03:20:00Z", 1.0, null, null, null, null, null);
        }

        List<PredictionHistoryResponse> result = repository.findRecent("BTCUSDT", "1m", 20);

        assertThat(result).hasSize(5);
        assertThat(result).allSatisfy(r -> assertThat(r.models()).hasSize(1));
        verifyQueryCount(2);
    }
}
