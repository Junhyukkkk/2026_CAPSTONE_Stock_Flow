package com.stockflow.core.metrics;

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
 */
@Slf4j
@Component
@Getter
public class PerformanceMetrics {

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

    // E2E Latency (메시지 timestamp → 처리 완료 시점)
    private final ConcurrentLinkedDeque<Long> e2eLatencies = new ConcurrentLinkedDeque<>();
    private final LongAdder totalE2ELatency = new LongAdder();
    private final AtomicLong e2eLatencyCount = new AtomicLong(0);
    private final AtomicLong minE2ELatency = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxE2ELatency = new AtomicLong(0);

    private static final int MAX_LATENCY_SAMPLES = 100000;

    // Timestamp 단위 판별 기준 (자릿수)
    private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;      // 13자리 시작 (2001년~)
    private static final long MICROS_THRESHOLD = 1_000_000_000_000_000L;  // 16자리 시작
    private static final long NANOS_THRESHOLD = 1_000_000_000_000_000_000L; // 19자리 시작

    /**
     * timestamp를 밀리초 단위로 정규화
     * - 나노초 (19자리): /1,000,000
     * - 마이크로초 (16자리): /1,000
     * - 밀리초 (13자리): 그대로
     */
    private long normalizeToMillis(long timestamp) {
        if (timestamp >= NANOS_THRESHOLD) {
            return timestamp / 1_000_000;  // 나노초 → 밀리초
        } else if (timestamp >= MICROS_THRESHOLD) {
            return timestamp / 1_000;      // 마이크로초 → 밀리초
        } else {
            return timestamp;              // 이미 밀리초
        }
    }

    /**
     * 처리 성공 기록
     */
    public void recordSuccess() {
        totalProcessed.increment();
        lastProcessedTime.set(System.currentTimeMillis());
        recordThreadCount();
    }

    /**
     * 처리 성공 기록 + E2E Latency 측정
     *
     * @param messageTimestamp 메시지 원본 timestamp (epoch ms)
     */
    public void recordSuccessWithLatency(long messageTimestamp) {
        recordSuccess();
        recordE2ELatency(messageTimestamp);
    }

    /**
     * 스레드별 처리 건수 기록
     */
    private void recordThreadCount() {
        String threadName = Thread.currentThread().getName();
        threadProcessedCount.computeIfAbsent(threadName, k -> new LongAdder()).increment();
    }

    /**
     * E2E Latency 기록 (메시지 timestamp → 처리 완료 시점)
     *
     * @param messageTimestamp 메시지 원본 timestamp (자동 단위 감지: ms, μs, ns)
     */
    public void recordE2ELatency(long messageTimestamp) {
        long now = System.currentTimeMillis();
        long timestampMs = normalizeToMillis(messageTimestamp);
        long latency = now - timestampMs;

        e2eLatencyCount.incrementAndGet();
        totalE2ELatency.add(latency);

        minE2ELatency.updateAndGet(current -> Math.min(current, latency));
        maxE2ELatency.updateAndGet(current -> Math.max(current, latency));

        // 샘플 저장 (메모리 제한)
        if (e2eLatencies.size() < MAX_LATENCY_SAMPLES) {
            e2eLatencies.addLast(latency);
        } else {
            e2eLatencies.pollFirst();
            e2eLatencies.addLast(latency);
        }
    }

    /**
     * 처리 실패 기록
     */
    public void recordFailure() {
        totalFailed.increment();
        recordThreadCount();
    }

    /**
     * 처리 시간 기록
     *
     * @param processingTimeMs 처리 시간 (밀리초)
     */
    public void recordProcessingTime(long processingTimeMs) {
        processingCount.incrementAndGet();
        totalProcessingTime.add(processingTimeMs);

        // 최소/최대 업데이트
        minProcessingTime.updateAndGet(current ->
            Math.min(current, processingTimeMs));
        maxProcessingTime.updateAndGet(current ->
            Math.max(current, processingTimeMs));
    }

    /**
     * 초당 처리량 계산
     */
    public double getThroughputPerSecond() {
        long elapsed = System.currentTimeMillis() - lastProcessedTime.get();
        if (elapsed == 0) {
            return 0;
        }
        return (double) totalProcessed.sum() / (elapsed / 1000.0);
    }

    /**
     * 평균 처리 시간 계산
     */
    public double getAverageProcessingTime() {
        long count = processingCount.get();
        if (count == 0) {
            return 0;
        }
        return (double) totalProcessingTime.sum() / count;
    }

    /**
     * 에러율 계산
     */
    public double getErrorRate() {
        long total = totalProcessed.sum() + totalFailed.sum();
        if (total == 0) {
            return 0;
        }
        return (double) totalFailed.sum() / total * 100;
    }

    /**
     * E2E Latency 평균 계산
     */
    public double getAverageE2ELatency() {
        long count = e2eLatencyCount.get();
        if (count == 0) {
            return 0;
        }
        return (double) totalE2ELatency.sum() / count;
    }

    /**
     * E2E Latency Percentile 계산
     *
     * @param percentile 0-100 (예: 50 for p50, 99 for p99)
     * @return 해당 percentile의 latency (ms)
     */
    public long getE2ELatencyPercentile(int percentile) {
        if (e2eLatencies.isEmpty()) {
            return 0;
        }

        List<Long> sorted = new ArrayList<>(e2eLatencies);
        Collections.sort(sorted);

        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));

        return sorted.get(index);
    }

    /**
     * 스레드별 처리 건수 조회
     */
    public Map<String, Long> getThreadStats() {
        Map<String, Long> stats = new HashMap<>();
        threadProcessedCount.forEach((thread, count) -> stats.put(thread, count.sum()));
        return stats;
    }

    /**
     * 전체 메트릭 스냅샷 조회
     */
    public Map<String, Object> getMetricsSnapshot() {
        Map<String, Object> snapshot = new LinkedHashMap<>();

        // 기본 처리량
        snapshot.put("totalProcessed", totalProcessed.sum());
        snapshot.put("totalFailed", totalFailed.sum());
        snapshot.put("errorRate", String.format("%.2f%%", getErrorRate()));

        // 처리 시간 (elapsed)
        long elapsedMs = System.currentTimeMillis() - startTime.get();
        snapshot.put("elapsedMs", elapsedMs);
        snapshot.put("throughputPerSecond", String.format("%.2f",
            elapsedMs > 0 ? (totalProcessed.sum() * 1000.0 / elapsedMs) : 0));

        // Processing Time (내부 처리 시간)
        Map<String, Object> processingTime = new LinkedHashMap<>();
        processingTime.put("avg", String.format("%.2f ms", getAverageProcessingTime()));
        processingTime.put("min", minProcessingTime.get() == Long.MAX_VALUE ? "N/A" : minProcessingTime.get() + " ms");
        processingTime.put("max", maxProcessingTime.get() + " ms");
        snapshot.put("processingTime", processingTime);

        // E2E Latency
        Map<String, Object> e2eLatency = new LinkedHashMap<>();
        e2eLatency.put("count", e2eLatencyCount.get());
        e2eLatency.put("avg", String.format("%.2f ms", getAverageE2ELatency()));
        e2eLatency.put("min", minE2ELatency.get() == Long.MAX_VALUE ? "N/A" : minE2ELatency.get() + " ms");
        e2eLatency.put("max", maxE2ELatency.get() + " ms");
        e2eLatency.put("p50", getE2ELatencyPercentile(50) + " ms");
        e2eLatency.put("p90", getE2ELatencyPercentile(90) + " ms");
        e2eLatency.put("p99", getE2ELatencyPercentile(99) + " ms");
        snapshot.put("e2eLatency", e2eLatency);

        // 스레드별 처리량
        snapshot.put("threadStats", getThreadStats());

        return snapshot;
    }

    /**
     * 메트릭 리셋
     */
    public void reset() {
        totalProcessed.reset();
        totalFailed.reset();
        minProcessingTime.set(Long.MAX_VALUE);
        maxProcessingTime.set(0);
        totalProcessingTime.reset();
        processingCount.set(0);
        lastProcessedTime.set(System.currentTimeMillis());
        startTime.set(System.currentTimeMillis());

        // 스레드별 카운트 초기화
        threadProcessedCount.clear();

        // E2E Latency 초기화
        e2eLatencies.clear();
        totalE2ELatency.reset();
        e2eLatencyCount.set(0);
        minE2ELatency.set(Long.MAX_VALUE);
        maxE2ELatency.set(0);
    }

    /**
     * 메트릭 로깅
     */
    public void logMetrics() {
        log.info("Performance Metrics - Throughput: {:.2f} msg/s, Avg Processing Time: {:.2f}ms, " +
            "Min: {}ms, Max: {}ms, Error Rate: {:.2f}%, Total Processed: {}, Total Failed: {}",
            String.format("%.2f", getThroughputPerSecond()),
            String.format("%.2f", getAverageProcessingTime()),
            minProcessingTime.get() == Long.MAX_VALUE ? 0 : minProcessingTime.get(),
            maxProcessingTime.get(),
            String.format("%.2f", getErrorRate()),
            totalProcessed.sum(),
            totalFailed.sum());
    }
}
