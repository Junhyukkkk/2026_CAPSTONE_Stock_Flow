package com.stockflow.core.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PerformanceMetrics 테스트
 *
 * - 음수 E2E latency(시계 skew) 표본이 집계를 오염시키지 않고 버려지는지
 * - 퍼센타일 계산이 (구 deque 기반 대신) DistributionSummary 로 정상 동작하는지
 */
class PerformanceMetricsTest {

    private PerformanceMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new PerformanceMetrics(new SimpleMeterRegistry());
    }

    @Test
    void negativeE2ELatency_isDiscarded_notCountedOrAveraged() {
        // 정상 표본 하나
        metrics.recordE2ELatency(System.currentTimeMillis() - 100);

        // 시계 skew 로 인한 음수 표본 (미래 timestamp)
        metrics.recordE2ELatency(System.currentTimeMillis() + 60_000);

        assertEquals(1, metrics.getE2eLatencyCount().get(),
            "음수 latency 표본은 카운트에 포함되면 안 된다");
        assertTrue(metrics.getAverageE2ELatency() < 60_000,
            "음수 표본이 평균을 오염시키면 안 된다");
        assertTrue(metrics.getAverageE2ELatency() >= 0);
    }

    @Test
    void positiveE2ELatency_isRecorded() {
        metrics.recordE2ELatency(System.currentTimeMillis() - 50);
        assertEquals(1, metrics.getE2eLatencyCount().get());
        assertTrue(metrics.getAverageE2ELatency() >= 0);
    }

    @Test
    void percentile_withNoSamples_returnsZero() {
        assertEquals(0L, metrics.getE2ELatencyPercentile(50));
        assertEquals(0L, metrics.getE2ELatencyPercentile(99));
    }

    @Test
    void percentile_withSamples_returnsNonNegativeApproximation() {
        for (int i = 0; i < 50; i++) {
            metrics.recordE2ELatency(System.currentTimeMillis() - (i + 1));
        }
        // DistributionSummary 기반 근사치이므로 정확한 값이 아니라 합리적 범위만 검증한다.
        assertTrue(metrics.getE2ELatencyPercentile(50) >= 0);
        assertTrue(metrics.getE2ELatencyPercentile(99) >= 0);
    }

    @Test
    void reset_clearsE2ELatencyState() {
        metrics.recordE2ELatency(System.currentTimeMillis() - 100);
        metrics.reset();
        assertEquals(0, metrics.getE2eLatencyCount().get());
        assertEquals(0.0, metrics.getAverageE2ELatency());
    }
}
