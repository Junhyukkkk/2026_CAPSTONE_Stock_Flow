package com.stockflow.core.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 파이프라인 구간별 지연 계측
 *
 * PerformanceMetrics가 뭉뚱그린 E2E 지연을 구간 단위로 쪼개기 위한 계측기.
 * 어느 구간이 실제 병목인지 원인 귀속을 하려면 구간별 분해가 필요하다.
 *
 * Prometheus 노출: stockflow_stage_seconds{stage="..."} (p50/p90/p99)
 *
 * 구간 이름은 {@link Stage} 상수를 사용한다.
 * Redis SET/PUBLISH 등은 서브 밀리초 단위라 나노초로 기록한다.
 */
@Component
public class PipelineStageMetrics {

    private static final String METER_NAME = "stockflow.stage";

    /** 구간 이름 (Prometheus의 stage 태그 값) */
    public static final class Stage {
        // 실시간 경로
        public static final String REDIS_PREV_CLOSE_GET = "redis.prev_close_get";
        public static final String REDIS_SET_LATEST = "redis.set_latest";
        public static final String REDIS_PUBLISH = "redis.publish";
        /** SET+PUBLISH 를 파이프라인 1회 왕복으로 묶었을 때의 구간 */
        public static final String REDIS_SET_PUBLISH_PIPELINED = "redis.set_publish_pipelined";
        public static final String SNAPSHOT_SERIALIZE = "snapshot.serialize";
        public static final String REALTIME_TOTAL = "realtime.total";

        // Pub/Sub -> WebSocket 홉
        public static final String WS_DISPATCH = "ws.dispatch";

        // 저장 경로
        public static final String STORAGE_IDEMPOTENCY_CHECK = "storage.idempotency_check";
        public static final String STORAGE_IDEMPOTENCY_MARK = "storage.idempotency_mark";
        public static final String STORAGE_TX_TOTAL = "storage.tx_total";
        public static final String STORAGE_DB_INSERT = "storage.db_insert";
        public static final String STORAGE_INSTRUMENT_REGISTRY = "storage.instrument_registry";

        private Stage() {
        }
    }

    private final MeterRegistry registry;
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();

    public PipelineStageMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    private Timer timer(String stage) {
        return timers.computeIfAbsent(stage, s -> Timer.builder(METER_NAME)
                .tag("stage", s)
                .description("Pipeline stage latency")
                .publishPercentiles(0.5, 0.9, 0.99)
                .register(registry));
    }

    /** 구간 시작 시각 (나노초). {@link #record(String, long)}와 짝으로 사용한다. */
    public long start() {
        return System.nanoTime();
    }

    public void record(String stage, long startNanos) {
        timer(stage).record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    /** 반환값이 있는 구간 측정 */
    public <T> T time(String stage, Supplier<T> operation) {
        long start = System.nanoTime();
        try {
            return operation.get();
        } finally {
            record(stage, start);
        }
    }

    /** 반환값이 없는 구간 측정 */
    public void time(String stage, Runnable operation) {
        long start = System.nanoTime();
        try {
            operation.run();
        } finally {
            record(stage, start);
        }
    }
}
