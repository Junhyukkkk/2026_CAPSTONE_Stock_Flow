package com.stockflow.realtime.prediction;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.prediction.PredictionHistoryRepository.PointRecord;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PredictionHistoryRecorderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static JsonNode json(String text) throws Exception {
        return MAPPER.readTree(text);
    }

    private static String model(String name, String metrics, String... forecastTs) {
        StringBuilder forecast = new StringBuilder();
        for (String ts : forecastTs) {
            if (forecast.length() > 0) {
                forecast.append(',');
            }
            forecast.append("{\"ts\":\"").append(ts).append("\",\"yhat\":10.5,\"yhat_lower\":10.0,\"yhat_upper\":11.0}");
        }
        return "{\"model\":\"" + name + "\",\"metrics\":" + metrics
                + ",\"trained_at\":\"2026-10-05T03:11:14.63+00:00\",\"forecast\":[" + forecast + "]}";
    }

    private static final String[] THREE = {
            "2026-10-05T03:08:00+00:00", "2026-10-05T03:09:00+00:00", "2026-10-05T03:10:00+00:00"};

    @Test
    void 세_모델_세_점_응답은_아홉_개_포인트로_변환되고_metrics가_없으면_null() throws Exception {
        JsonNode response = json("{\"last_ts\":\"2026-10-05T03:07:00+00:00\",\"last_value\":86458.0,\"models\":["
                + model("ARIMA", "null", THREE) + ","
                + model("Log-return ARIMA", "{\"mae\":71.67,\"rmse\":89.6,\"test_size\":30}", THREE) + ","
                + model("Chronos-Bolt", "{}", THREE) + "]}");

        PredictionHistoryRecorder.Converted converted =
                PredictionHistoryRecorder.convert("btcusdt", "1m", 3, null, response, 42);

        assertThat(converted.run().symbol()).isEqualTo("BTCUSDT");
        assertThat(converted.run().source()).isEmpty();
        assertThat(converted.run().baseTs()).isEqualTo(Instant.parse("2026-10-05T03:07:00Z"));
        assertThat(converted.run().baseValue()).isEqualTo(86458.0);
        assertThat(converted.run().latencyMs()).isEqualTo(42);
        assertThat(converted.run().rawModels()).contains("Chronos-Bolt");
        assertThat(converted.points()).hasSize(9);
        List<PointRecord> arima = converted.points().stream().filter(p -> p.model().equals("ARIMA")).toList();
        assertThat(arima).hasSize(3).allSatisfy(p -> {
            assertThat(p.mae()).isNull();
            assertThat(p.rmse()).isNull();
        });
        assertThat(converted.points().stream().filter(p -> p.model().equals("Log-return ARIMA")))
                .hasSize(3).allSatisfy(p -> {
                    assertThat(p.mae()).isEqualTo(71.67);
                    assertThat(p.rmse()).isEqualTo(89.6);
                });
        assertThat(converted.points().stream().filter(p -> p.model().equals("Chronos-Bolt")))
                .allSatisfy(p -> assertThat(p.mae()).isNull());
        PointRecord first = arima.get(0);
        assertThat(first.ts()).isEqualTo(Instant.parse("2026-10-05T03:08:00Z"));
        assertThat(first.yhatLower()).isEqualTo(10.0);
        assertThat(first.trainedAt()).isNotNull();
    }

    @Test
    void 잘못된_ts_점과_잘못된_모델은_건너뛴다() throws Exception {
        JsonNode response = json("{\"last_ts\":\"2026-10-05T03:07:00Z\",\"models\":["
                + model("A", "null", "2026-10-05T03:08:00Z", "not-a-date", "2026-10-05T03:08:00Z") + ","
                + "{\"model\":\"B\"},"
                + "{\"forecast\":[]},"
                + model("C", "null", "2026-10-05T03:09:00Z") + "]}");

        PredictionHistoryRecorder.Converted converted =
                PredictionHistoryRecorder.convert("BTCUSDT", "1m", 3, "s", response, 1);

        assertThat(converted.points()).extracting(PointRecord::model, PointRecord::ts)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("A", Instant.parse("2026-10-05T03:08:00Z")),
                        org.assertj.core.groups.Tuple.tuple("C", Instant.parse("2026-10-05T03:09:00Z")));
        assertThat(converted.run().baseValue()).isNull();
    }

    @Test
    void models가_없으면_포인트_없는_run이고_last_ts가_이상하면_null() throws Exception {
        PredictionHistoryRecorder.Converted noModels = PredictionHistoryRecorder.convert(
                "BTCUSDT", "1m", 3, null, json("{\"last_ts\":\"2026-10-05T03:07:00Z\"}"), 1);
        assertThat(noModels.points()).isEmpty();
        assertThat(noModels.run().rawModels()).isNull();

        assertThat(PredictionHistoryRecorder.convert(
                "BTCUSDT", "1m", 3, null, json("{\"last_ts\":\"nope\",\"models\":[]}"), 1)).isNull();
    }

    @Test
    void 정상_경로에서는_변환_결과를_repository에_저장한다() throws Exception {
        PredictionHistoryRepository repository = mock(PredictionHistoryRepository.class);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        try {
            PredictionHistoryRecorder recorder =
                    new PredictionHistoryRecorder(repository, executor, new SimpleMeterRegistry());
            recorder.record("btcusdt", "1m", 3, null, json("{\"last_ts\":\"2026-10-05T03:07:00Z\",\"models\":["
                    + model("A", "null", THREE) + "]}"), 5);

            ArgumentCaptor<List<PointRecord>> points = ArgumentCaptor.forClass(List.class);
            verify(repository, timeout(2000)).save(any(), points.capture());
            assertThat(points.getValue()).hasSize(3);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void 큐가_가득_차면_버리고_카운터를_올리며_호출_스레드는_블록되지_않는다() throws Exception {
        PredictionHistoryRepository repository = mock(PredictionHistoryRepository.class);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(repository.save(any(), any())).thenAnswer(invocation -> {
            started.countDown();
            release.await(10, TimeUnit.SECONDS);
            return true;
        });
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1),
                new ThreadPoolExecutor.AbortPolicy());
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PredictionHistoryRecorder recorder = new PredictionHistoryRecorder(repository, executor, registry);
        JsonNode response = json("{\"last_ts\":\"2026-10-05T03:07:00Z\",\"models\":[" + model("A", "null", THREE) + "]}");
        try {
            recorder.record("BTCUSDT", "1m", 3, null, response, 1); // 실행 중(블록)
            assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
            recorder.record("BTCUSDT", "1m", 3, null, response, 1); // 큐 1칸

            long begin = System.nanoTime();
            for (int i = 0; i < 5; i++) {
                recorder.record("BTCUSDT", "1m", 3, null, response, 1); // 전부 drop
            }
            long elapsedMs = (System.nanoTime() - begin) / 1_000_000;

            assertThat(elapsedMs).isLessThan(500);
            assertThat(registry.get(PredictionHistoryRecorder.DROPPED_COUNTER).counter().count()).isEqualTo(5.0);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void repository가_예외를_던져도_삼킨다() throws Exception {
        PredictionHistoryRepository repository = mock(PredictionHistoryRepository.class);
        CountDownLatch called = new CountDownLatch(1);
        when(repository.save(any(), any())).thenAnswer(invocation -> {
            called.countDown();
            throw new IllegalStateException("db down");
        });
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(10));
        try {
            PredictionHistoryRecorder recorder =
                    new PredictionHistoryRecorder(repository, executor, new SimpleMeterRegistry());
            recorder.record("BTCUSDT", "1m", 3, null,
                    json("{\"last_ts\":\"2026-10-05T03:07:00Z\",\"models\":[]}"), 1);
            assertThat(called.await(2, TimeUnit.SECONDS)).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }
}
