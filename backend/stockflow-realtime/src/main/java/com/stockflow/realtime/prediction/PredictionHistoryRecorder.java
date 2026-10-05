package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.stockflow.realtime.prediction.PredictionHistoryRepository.PointRecord;
import com.stockflow.realtime.prediction.PredictionHistoryRepository.RunRecord;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * compare 응답을 별도 스레드에서 DB 에 저장한다. 저장 실패·큐 포화는 로그만 남기고 삼킨다.
 */
@Slf4j
@Component
public class PredictionHistoryRecorder {

    static final String DROPPED_COUNTER = "prediction.history.dropped";
    private static final int MODEL_MAX_LENGTH = 64;
    private static final int SOURCE_MAX_LENGTH = 32;

    record Converted(RunRecord run, List<PointRecord> points) {
    }

    private final PredictionHistoryRepository repository;
    private final Executor executor;
    private final Counter droppedCounter;

    public PredictionHistoryRecorder(
            PredictionHistoryRepository repository,
            @Qualifier("predictionHistoryExecutor") Executor executor,
            MeterRegistry meterRegistry) {
        this.repository = repository;
        this.executor = executor;
        this.droppedCounter = Counter.builder(DROPPED_COUNTER)
                .description("Prediction history saves dropped because the queue was full")
                .register(meterRegistry);
    }

    /** 호출 스레드를 블록하거나 예외를 던지지 않는다. 큐가 가득 차면 버린다. */
    public void record(String symbol, String interval, int horizon, String source,
                       JsonNode response, long latencyMs) {
        try {
            executor.execute(() -> persist(symbol, interval, horizon, source, response, latencyMs));
        } catch (RejectedExecutionException e) {
            droppedCounter.increment();
            log.warn("예측 이력 저장 큐가 가득 차 버립니다. symbol={} interval={} horizon={}",
                    symbol, interval, horizon);
        } catch (RuntimeException e) {
            droppedCounter.increment();
            log.warn("예측 이력 저장 요청을 접수하지 못했습니다. symbol={} interval={} horizon={}",
                    symbol, interval, horizon, e);
        }
    }

    private void persist(String symbol, String interval, int horizon, String source,
                         JsonNode response, long latencyMs) {
        try {
            Converted converted = convert(symbol, interval, horizon, source, response, latencyMs);
            if (converted == null) {
                return;
            }
            boolean saved = repository.save(converted.run(), converted.points());
            log.debug("예측 이력 저장 symbol={} interval={} horizon={} saved={} points={}",
                    converted.run().symbol(), interval, horizon, saved, converted.points().size());
        } catch (Exception e) {
            log.warn("예측 이력 저장 실패 symbol={} interval={} horizon={}", symbol, interval, horizon, e);
        }
    }

    /** 기준 시각(last_ts)을 읽을 수 없으면 null. 이상한 모델/점은 건너뛴다. */
    static Converted convert(String symbol, String interval, int horizon, String source,
                             JsonNode response, long latencyMs) {
        Instant baseTs = parseInstant(response.path("last_ts"));
        if (baseTs == null) {
            log.warn("예측 이력 건너뜀: last_ts 를 읽을 수 없습니다. symbol={} interval={}", symbol, interval);
            return null;
        }
        JsonNode models = response.path("models");
        String normalizedSource = source == null ? "" : source.strip();
        if (normalizedSource.length() > SOURCE_MAX_LENGTH) {
            normalizedSource = normalizedSource.substring(0, SOURCE_MAX_LENGTH);
        }
        RunRecord run = new RunRecord(
                symbol.toUpperCase(), interval, horizon, normalizedSource, baseTs,
                parseDouble(response.path("last_value")), (int) Math.min(latencyMs, Integer.MAX_VALUE),
                models.isArray() ? models.toString() : null);

        List<PointRecord> points = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        if (models.isArray()) {
            for (JsonNode model : models) {
                collectModelPoints(model, points, seen);
            }
        } else {
            log.warn("예측 이력: models 배열이 없습니다. symbol={} interval={}", symbol, interval);
        }
        return new Converted(run, points);
    }

    private static void collectModelPoints(JsonNode model, List<PointRecord> points, Set<String> seen) {
        String name = model.path("model").asText("").strip();
        JsonNode forecast = model.path("forecast");
        if (name.isEmpty() || !forecast.isArray()) {
            log.warn("예측 이력: 모델 이름 또는 forecast 가 올바르지 않아 건너뜁니다. model='{}'", name);
            return;
        }
        if (name.length() > MODEL_MAX_LENGTH) {
            name = name.substring(0, MODEL_MAX_LENGTH);
        }
        JsonNode metrics = model.path("metrics");
        Double mae = parseDouble(metrics.path("mae"));
        Double rmse = parseDouble(metrics.path("rmse"));
        Instant trainedAt = parseInstant(model.path("trained_at"));

        for (JsonNode point : forecast) {
            Instant ts = parseInstant(point.path("ts"));
            Double yhat = parseDouble(point.path("yhat"));
            if (ts == null || yhat == null) {
                log.warn("예측 이력: ts/yhat 가 올바르지 않은 점을 건너뜁니다. model='{}'", name);
                continue;
            }
            if (!seen.add(name + "|" + ts)) {
                continue; // PK(run_id, model, ts) 충돌 방지
            }
            points.add(new PointRecord(name, ts, yhat,
                    parseDouble(point.path("yhat_lower")), parseDouble(point.path("yhat_upper")),
                    trainedAt, mae, rmse));
        }
    }

    private static Instant parseInstant(JsonNode node) {
        if (!node.isTextual()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(node.asText()).toInstant();
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static Double parseDouble(JsonNode node) {
        if (!node.isNumber()) {
            return null;
        }
        double value = node.asDouble();
        return Double.isFinite(value) ? value : null;
    }
}
