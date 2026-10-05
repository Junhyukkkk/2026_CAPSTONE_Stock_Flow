package com.stockflow.realtime.prediction;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.List;

/**
 * 예측 실행(prediction_runs)과 모델별 예측점(prediction_forecast_points) 저장.
 */
@Repository
public class PredictionHistoryRepository {

    /** 분석 서버 한 번의 compare 응답에서 뽑은 실행 헤더. */
    public record RunRecord(String symbol, String interval, int horizon, String source,
                            Instant baseTs, Double baseValue, Integer latencyMs, String rawModels) {
    }

    /** 모델의 예측점 하나. mae/rmse 는 모델 단위 값을 모든 점에 같이 채운다. */
    public record PointRecord(String model, Instant ts, double yhat, Double yhatLower, Double yhatUpper,
                              Instant trainedAt, Double mae, Double rmse) {
    }

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public PredictionHistoryRepository(
            JdbcTemplate jdbcTemplate,
            @Qualifier("storageJdbcTransactionTemplate") TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * run 과 points 를 한 트랜잭션으로 저장한다. 같은 (symbol, interval, horizon, source, base_ts) 가
     * 이미 있으면 아무것도 넣지 않고 false 를 반환한다.
     */
    public boolean save(RunRecord run, List<PointRecord> points) {
        Boolean saved = transactionTemplate.execute(status -> {
            List<Long> ids = jdbcTemplate.query(
                    """
                    INSERT INTO prediction_runs
                        (symbol, "interval", horizon, source, base_ts, base_value, latency_ms, raw_models)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))
                    ON CONFLICT (symbol, "interval", horizon, source, base_ts) DO NOTHING
                    RETURNING id
                    """,
                    (rs, rowNum) -> rs.getLong(1),
                    run.symbol(), run.interval(), run.horizon(), run.source(),
                    Timestamp.from(run.baseTs()), run.baseValue(), run.latencyMs(), run.rawModels());
            if (ids.isEmpty()) {
                return false;
            }
            insertPoints(ids.get(0), points);
            return true;
        });
        return Boolean.TRUE.equals(saved);
    }

    private void insertPoints(long runId, List<PointRecord> points) {
        if (points.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO prediction_forecast_points
                    (run_id, model, ts, yhat, yhat_lower, yhat_upper, trained_at, mae, rmse)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                points, points.size(),
                (ps, p) -> {
                    ps.setLong(1, runId);
                    ps.setString(2, p.model());
                    ps.setTimestamp(3, Timestamp.from(p.ts()));
                    ps.setDouble(4, p.yhat());
                    setNullableDouble(ps, 5, p.yhatLower());
                    setNullableDouble(ps, 6, p.yhatUpper());
                    if (p.trainedAt() == null) {
                        ps.setNull(7, Types.TIMESTAMP_WITH_TIMEZONE);
                    } else {
                        ps.setTimestamp(7, Timestamp.from(p.trainedAt()));
                    }
                    setNullableDouble(ps, 8, p.mae());
                    setNullableDouble(ps, 9, p.rmse());
                });
    }

    private static void setNullableDouble(PreparedStatement ps, int index, Double value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DOUBLE);
        } else {
            ps.setDouble(index, value);
        }
    }
}
