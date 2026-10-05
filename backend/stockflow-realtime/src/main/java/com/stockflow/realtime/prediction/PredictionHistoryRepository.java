package com.stockflow.realtime.prediction;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    /**
     * 심볼·interval 의 최근 run 을 base_ts 내림차순으로 최대 limit 개 조회한다.
     * run 목록 1회 + 해당 run 들의 points 1회, 총 2회 쿼리로 메모리에서 조립한다.
     */
    public List<PredictionHistoryResponse> findRecent(String symbol, String interval, int limit) {
        List<RunRow> runs = jdbcTemplate.query(
                """
                SELECT id, symbol, "interval", horizon, source, base_ts, base_value, requested_at, latency_ms
                FROM prediction_runs
                WHERE symbol = ? AND "interval" = ?
                ORDER BY base_ts DESC, id DESC
                LIMIT ?
                """,
                (rs, rowNum) -> new RunRow(
                        rs.getLong("id"), rs.getString("symbol"), rs.getString("interval"),
                        rs.getInt("horizon"), rs.getString("source"),
                        rs.getTimestamp("base_ts").toInstant(), getNullableDouble(rs, "base_value"),
                        rs.getTimestamp("requested_at").toInstant(), getNullableInt(rs, "latency_ms")),
                symbol, interval, limit);
        if (runs.isEmpty()) {
            return List.of();
        }

        String placeholders = runs.stream().map(r -> "?").collect(Collectors.joining(", "));
        List<PointRow> pointRows = jdbcTemplate.query(
                "SELECT run_id, model, ts, yhat, yhat_lower, yhat_upper, trained_at, mae, rmse "
                        + "FROM prediction_forecast_points WHERE run_id IN (" + placeholders + ") "
                        + "ORDER BY run_id, model, ts",
                (rs, rowNum) -> {
                    Timestamp trainedAt = rs.getTimestamp("trained_at");
                    return new PointRow(
                            rs.getLong("run_id"), rs.getString("model"), rs.getTimestamp("ts").toInstant(),
                            rs.getDouble("yhat"), getNullableDouble(rs, "yhat_lower"),
                            getNullableDouble(rs, "yhat_upper"),
                            trainedAt == null ? null : trainedAt.toInstant(),
                            getNullableDouble(rs, "mae"), getNullableDouble(rs, "rmse"));
                },
                runs.stream().map(RunRow::id).toArray());

        Map<Long, Map<String, List<PointRow>>> byRunAndModel = new LinkedHashMap<>();
        for (PointRow p : pointRows) {
            byRunAndModel.computeIfAbsent(p.runId(), k -> new LinkedHashMap<>())
                    .computeIfAbsent(p.model(), k -> new ArrayList<>())
                    .add(p);
        }

        List<PredictionHistoryResponse> result = new ArrayList<>(runs.size());
        for (RunRow run : runs) {
            List<PredictionHistoryResponse.ModelForecast> models = new ArrayList<>();
            byRunAndModel.getOrDefault(run.id(), Collections.emptyMap()).forEach((model, rows) -> {
                PointRow first = rows.get(0);
                models.add(new PredictionHistoryResponse.ModelForecast(
                        model, first.trainedAt(), first.mae(), first.rmse(),
                        rows.stream()
                                .map(p -> new PredictionHistoryResponse.Point(
                                        p.ts(), p.yhat(), p.yhatLower(), p.yhatUpper()))
                                .toList()));
            });
            result.add(new PredictionHistoryResponse(
                    run.id(), run.symbol(), run.interval(), run.horizon(), run.source(),
                    run.baseTs(), run.baseValue(), run.requestedAt(), run.latencyMs(), models));
        }
        return result;
    }

    private record RunRow(long id, String symbol, String interval, int horizon, String source,
                          Instant baseTs, Double baseValue, Instant requestedAt, Integer latencyMs) {
    }

    private record PointRow(long runId, String model, Instant ts, double yhat, Double yhatLower,
                            Double yhatUpper, Instant trainedAt, Double mae, Double rmse) {
    }

    private static Double getNullableDouble(ResultSet rs, String column) throws SQLException {
        double value = rs.getDouble(column);
        return rs.wasNull() ? null : value;
    }

    private static Integer getNullableInt(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        return rs.wasNull() ? null : value;
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
