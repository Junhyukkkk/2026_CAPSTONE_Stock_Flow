package com.stockflow.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 성능 메트릭 수집
 *
 * 처리량, 지연시간, 에러율 등을 추적
 * Micrometer MeterRegistry에 등록하여 Prometheus에서 수집 가능
 *
 * Prometheus 메트릭 목록:
 * - stockflow_total_processed: 총 처리 성공 건수
 * - stockflow_total_failed: 총 처리 실패 건수
 * - stockflow_error_rate: 에러율 (%)
 * - stockflow_throughput_per_second: 초당 처리량
 * - stockflow_processing_time_avg/min/max: 처리 시간
 * - stockflow_e2e_latency_avg/p50/p90/p99: E2E 지연시간
 */
@Slf4j
@Component
@Getter
public class PerformanceMetrics {

    private static final String PREFIX = "stockflow";

    // 처리량 메트릭
    private final LongAdder totalProcessed = new LongAdder();
    private final LongAdder totalFailed = new LongAdder();
    private final AtomicLong lastProcessedTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong startTime = new AtomicLong(System.currentTimeMillis());

    // 지연시간 메트릭
    private final AtomicLong minProcessingTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxProcessingTime = new AtomicLong(0);
    private final LongAdder totalProcessingTime = new LongAdder();
    private final AtomicLong processingCount = new AtomicLong(0);

    // 스레드별 처리 건수
    private final ConcurrentHashMap<String, LongAdder> threadProcessedCount = new ConcurrentHashMap<>();

    // E2E Latency
    private final ConcurrentLinkedDeque<Long> e2eLatencies = new ConcurrentLinkedDeque<>();
    private final LongAdder totalE2ELatency = new LongAdder();
    private final AtomicLong e2eLatencyCount = new AtomicLong(0);
    private final AtomicLong minE2ELatency = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxE2ELatency = new AtomicLong(0);

    private static final int MAX_LATENCY_SAMPLES = 100000;

    private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;
    private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;
    private static final long NANOS_THRESHOLD = 1_000_000_000_000_000_000L;

    // Micrometer 메트릭
    private final Counter processedCounter;
    private final Counter failedCounter;
    private final DistributionSummary processingTimeSummary;
    private final DistributionSummary e2eLatencySummary;

    public PerformanceMetrics(MeterRegistry registry) {
        // Counter - 대시보드 이름에 맞춤
        this.processedCounter = Counter.builder(PREFIX + ".total.processed")
            .description("Total processed messages")
            .register(registry);

        this.failedCounter = Counter.builder(PREFIX + ".total.failed")
            .description("Total failed messages")
            .register(registry);

        // DistributionSummary (percentile 지원)
        this.processingTimeSummary = DistributionSummary.builder(PREFIX + ".processing.time.distribution")
            .description("Processing time distribution in milliseconds")
            .baseUnit("ms")
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .register(registry);

        this.e2eLatencySummary = DistributionSummary.builder(PREFIX + ".e2e.latency.distribution")
            .description("End-to-end latency distribution in milliseconds")
            .baseUnit("ms")
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .register(registry);

        // Gauge - 대시보드 이름에 맞춤
        Gauge.builder(PREFIX + ".throughput.per.second", this, PerformanceMetrics::getThroughputPerSecond)
            .description("Messages processed per second")
            .register(registry);

        Gauge.builder(PREFIX + ".error.rate", this, PerformanceMetrics::getErrorRate)
            .description("Error rate percentage")
            .register(registry);

        // Processing Time
        Gauge.builder(PREFIX + ".processing.time.avg", this, PerformanceMetrics::getAverageProcessingTime)
            .description("Average processing time in milliseconds")
            .register(registry);

        Gauge.builder(PREFIX + ".processing.time.min", this, m -> {
            long min = m.getMinProcessingTime().get();
            return min == Long.MAX_VALUE ? 0.0 : (double) min;
        }).description("Minimum processing time in milliseconds").register(registry);

        Gauge.builder(PREFIX + ".processing.time.max", this, m -> (double) m.getMaxProcessingTime().get())
            .description("Maximum processing time in milliseconds")
            .register(registry);

        // E2E Latency
        Gauge.builder(PREFIX + ".e2e.latency.avg", this, PerformanceMetrics::getAverageE2ELatency)
            .description("Average E2E latency in milliseconds")
            .register(registry);

        Gauge.builder(PREFIX + ".e2e.latency.p50", this, m -> (double) m.getE2ELatencyPercentile(50))
            .description("E2E latency 50th percentile")
            .register(registry);

        Gauge.builder(PREFIX + ".e2e.latency.p90", this, m -> (double) m.getE2ELatencyPercentile(90))
            .description("E2E latency 90th percentile")
            .register(registry);

        Gauge.builder(PREFIX + ".e2e.latency.p99", this, m -> (double) m.getE2ELatencyPercentile(99))
            .description("E2E latency 99th percentile")
            .register(registry);
    }

    private long normalizeToMillis(long timestamp) {
        if (timestamp >= NANOS_THRESHOLD) {
            return timestamp / 1_000_000;
        } else if (timestamp >= MICROS_THRESHOLD) {
            return timestamp / 1_000;
        } else {
            return timestamp;
        }
    }

    public void recordSuccess() {
        totalProcessed.increment();
        processedCounter.increment();
        lastProcessedTime.set(System.currentTimeMillis());
        recordThreadCount();
    }

    public void recordSuccessWithLatency(long messageTimestamp) {
        recordSuccess();
        recordE2ELatency(messageTimestamp);
    }

    private void recordThreadCount() {
        String threadName = Thread.currentThread().getName();
        threadProcessedCount.computeIfAbsent(threadName, k -> new LongAdder()).increment();
    }

    public void recordE2ELatency(long messageTimestamp) {
        long now = System.currentTimeMillis();
        long timestampMs = normalizeToMillis(messageTimestamp);
        long latency = now - timestampMs;

        e2eLatencyCount.incrementAndGet();
        totalE2ELatency.add(latency);
        e2eLatencySummary.record(latency);

        minE2ELatency.updateAndGet(current -> Math.min(current, latency));
        maxE2ELatency.updateAndGet(current -> Math.max(current, latency));

        if (e2eLatencies.size() < MAX_LATENCY_SAMPLES) {
            e2eLatencies.addLast(latency);
        } else {
            e2eLatencies.pollFirst();
            e2eLatencies.addLast(latency);
        }
    }

    public void recordFailure() {
        totalFailed.increment();
        failedCounter.increment();
        recordThreadCount();
    }

    public void recordProcessingTime(long processingTimeMs) {
        processingCount.incrementAndGet();
        totalProcessingTime.add(processingTimeMs);
        processingTimeSummary.record(processingTimeMs);

        minProcessingTime.updateAndGet(current -> Math.min(current, processingTimeMs));
        maxProcessingTime.updateAndGet(current -> Math.max(current, processingTimeMs));
    }

    public double getThroughputPerSecond() {
        long elapsed = System.currentTimeMillis() - startTime.get();
        if (elapsed == 0) return 0;
        return (double) totalProcessed.sum() * 1000 / elapsed;
    }

    public double getAverageProcessingTime() {
        long count = processingCount.get();
        if (count == 0) return 0;
        return (double) totalProcessingTime.sum() / count;
    }

    public double getErrorRate() {
        long total = totalProcessed.sum() + totalFailed.sum();
        if (total == 0) return 0;
        return (double) totalFailed.sum() / total * 100;
    }

    public double getAverageE2ELatency() {
        long count = e2eLatencyCount.get();
        if (count == 0) return 0;
        return (double) totalE2ELatency.sum() / count;
    }

    public long getE2ELatencyPercentile(int percentile) {
        if (e2eLatencies.isEmpty()) return 0;

        List<Long> sorted = new ArrayList<>(e2eLatencies);
        Collections.sort(sorted);

        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));

        return sorted.get(index);
    }

    public Map<String, Long> getThreadStats() {
        Map<String, Long> stats = new HashMap<>();
        threadProcessedCount.forEach((thread, count) -> stats.put(thread, count.sum()));
        return stats;
    }

    public Map<String, Object> getMetricsSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();

        snapshot.put("totalProcessed", totalProcessed.sum());
        snapshot.put("totalFailed", totalFailed.sum());
        snapshot.put("errorRate", String.format("%.2f%%", getErrorRate()));

        long elapsedMs = System.currentTimeMillis() - startTime.get();
        snapshot.put("elapsedMs", elapsedMs);
        snapshot.put("throughputPerSecond", String.format("%.2f", getThroughputPerSecond()));

        Map<String, Object> processingTime = new LinkedHashMap<>();
        processingTime.put("avg", String.format("%.2f ms", getAverageProcessingTime()));
        processingTime.put("min", minProcessingTime.get() == Long.MAX_VALUE ? "N/A" : minProcessingTime.get() + " ms");
        processingTime.put("max", maxProcessingTime.get() + " ms");
        snapshot.put("processingTime", processingTime);

        Map<String, Object> e2eLatency = new LinkedHashMap<>();
        e2eLatency.put("count", e2eLatencyCount.get());
        e2eLatency.put("avg", String.format("%.2f ms", getAverageE2ELatency()));
        e2eLatency.put("min", minE2ELatency.get() == Long.MAX_VALUE ? "N/A" : minE2ELatency.get() + " ms");
        e2eLatency.put("max", maxE2ELatency.get() + " ms");
        e2eLatency.put("p50", getE2ELatencyPercentile(50) + " ms");
        e2eLatency.put("p90", getE2ELatencyPercentile(90) + " ms");
        e2eLatency.put("p99", getE2ELatencyPercentile(99) + " ms");
        snapshot.put("e2eLatency", e2eLatency);

        snapshot.put("threadStats", getThreadStats());

        return snapshot;
    }

    public void reset() {
        totalProcessed.reset();
        totalFailed.reset();
        minProcessingTime.set(Long.MAX_VALUE);
        maxProcessingTime.set(0);
        totalProcessingTime.reset();
        processingCount.set(0);
        lastProcessedTime.set(System.currentTimeMillis());
        startTime.set(System.currentTimeMillis());
        threadProcessedCount.clear();
        e2eLatencies.clear();
        totalE2ELatency.reset();
        e2eLatencyCount.set(0);
        minE2ELatency.set(Long.MAX_VALUE);
        maxE2ELatency.set(0);
    }

    public void logMetrics() {
        log.info("Performance Metrics - Throughput: {} msg/s, Avg Processing Time: {}ms, " +
            "Min: {}ms, Max: {}ms, Error Rate: {}%, Total Processed: {}, Total Failed: {}",
            String.format("%.2f", getThroughputPerSecond()),
            String.format("%.2f", getAverageProcessingTime()),
            minProcessingTime.get() == Long.MAX_VALUE ? 0 : minProcessingTime.get(),
            maxProcessingTime.get(),
            String.format("%.2f", getErrorRate()),
            totalProcessed.sum(),
            totalFailed.sum());
    }
}
