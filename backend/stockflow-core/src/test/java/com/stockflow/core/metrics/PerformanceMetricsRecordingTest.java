package com.stockflow.core.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** 처리량/실패율/처리시간/스냅샷/게이지 동작. */
class PerformanceMetricsRecordingTest {

    private SimpleMeterRegistry registry;
    private PerformanceMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new PerformanceMetrics(registry);
    }

    @Test
    void normalizeToMillisHandlesEveryUnit() {
        assertThat(PerformanceMetrics.normalizeToMillis(1_700_000_000_000L)).isEqualTo(1_700_000_000_000L);
        assertThat(PerformanceMetrics.normalizeToMillis(1_700_000_000_000_000L)).isEqualTo(1_700_000_000_000L);
        assertThat(PerformanceMetrics.normalizeToMillis(1_700_000_000_000_000_000L)).isEqualTo(1_700_000_000_000L);
    }

    @Test
    void successAndFailureUpdateCountersAndErrorRate() {
        assertThat(metrics.getErrorRate()).isZero();

        metrics.recordSuccess();
        metrics.recordSuccess();
        metrics.recordSuccess();
        metrics.recordFailure();

        assertThat(metrics.getTotalProcessed().sum()).isEqualTo(3);
        assertThat(metrics.getTotalFailed().sum()).isEqualTo(1);
        assertThat(metrics.getErrorRate()).isEqualTo(25.0);
        assertThat(registry.get("stockflow.total.processed").counter().count()).isEqualTo(3);
        assertThat(registry.get("stockflow.total.failed").counter().count()).isEqualTo(1);
        assertThat(metrics.getThreadStats()).containsEntry(Thread.currentThread().getName(), 4L);
    }

    @Test
    void successWithLatencyRecordsE2e() {
        metrics.recordSuccessWithLatency(System.currentTimeMillis() - 50);

        assertThat(metrics.getTotalProcessed().sum()).isEqualTo(1);
        assertThat(metrics.getE2eLatencyCount().get()).isEqualTo(1);
        assertThat(metrics.getAverageE2ELatency()).isGreaterThanOrEqualTo(50);
    }

    @Test
    void processingTimeAggregates() {
        assertThat(metrics.getAverageProcessingTime()).isZero();

        metrics.recordProcessingTime(10);
        metrics.recordProcessingTime(30);

        assertThat(metrics.getAverageProcessingTime()).isEqualTo(20.0);
        assertThat(metrics.getMinProcessingTime().get()).isEqualTo(10);
        assertThat(metrics.getMaxProcessingTime().get()).isEqualTo(30);
    }

    @Test
    void throughputIsNonNegative() {
        metrics.recordSuccess();
        assertThat(metrics.getThroughputPerSecond()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void snapshotBeforeAndAfterTraffic() {
        Map<String, Object> empty = metrics.getMetricsSnapshot();
        assertThat(empty).containsEntry("totalProcessed", 0L);
        assertThat(((Map<?, ?>) empty.get("processingTime")).get("min")).isEqualTo("N/A");
        assertThat(((Map<?, ?>) empty.get("e2eLatency")).get("min")).isEqualTo("N/A");

        metrics.recordSuccessWithLatency(System.currentTimeMillis() - 20);
        metrics.recordProcessingTime(5);
        Map<String, Object> filled = metrics.getMetricsSnapshot();
        assertThat(filled).containsEntry("totalProcessed", 1L).containsKeys("elapsedMs", "threadStats");
        assertThat(((Map<?, ?>) filled.get("processingTime")).get("min")).isEqualTo("5 ms");
        assertThat(((Map<?, ?>) filled.get("e2eLatency")).get("count")).isEqualTo(1L);
    }

    @Test
    void gaugesReflectCurrentState() {
        assertThat(registry.get("stockflow.processing.time.min").gauge().value()).isZero();

        metrics.recordProcessingTime(8);
        metrics.recordE2ELatency(System.currentTimeMillis() - 10);

        assertThat(registry.get("stockflow.processing.time.min").gauge().value()).isEqualTo(8.0);
        assertThat(registry.get("stockflow.processing.time.max").gauge().value()).isEqualTo(8.0);
        assertThat(registry.get("stockflow.processing.time.avg").gauge().value()).isEqualTo(8.0);
        assertThat(registry.get("stockflow.e2e.latency.avg").gauge().value()).isGreaterThanOrEqualTo(10.0);
        registry.get("stockflow.e2e.latency.p50").gauge().value();
        registry.get("stockflow.e2e.latency.p90").gauge().value();
        registry.get("stockflow.e2e.latency.p99").gauge().value();
        registry.get("stockflow.throughput.per.second").gauge().value();
        registry.get("stockflow.error.rate").gauge().value();
    }

    @Test
    void resetClearsAllCounters() {
        metrics.recordSuccess();
        metrics.recordFailure();
        metrics.recordProcessingTime(3);

        metrics.reset();

        assertThat(metrics.getTotalProcessed().sum()).isZero();
        assertThat(metrics.getTotalFailed().sum()).isZero();
        assertThat(metrics.getProcessingCount().get()).isZero();
        assertThat(metrics.getThreadStats()).isEmpty();
    }

    @Test
    void logMetricsDoesNotThrow() {
        metrics.logMetrics();
        metrics.recordProcessingTime(2);
        metrics.logMetrics();
    }
}
