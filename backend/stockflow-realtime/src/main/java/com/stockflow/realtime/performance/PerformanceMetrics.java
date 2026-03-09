package com.stockflow.realtime.performance;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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

    // 지연시간 메트릭
    private final AtomicLong minProcessingTime = new AtomicLong(Long.MAX_VALUE);
    private final AtomicLong maxProcessingTime = new AtomicLong(0);
    private final LongAdder totalProcessingTime = new LongAdder();
    private final AtomicLong processingCount = new AtomicLong(0);

    /**
     * 처리 성공 기록
     */
    public void recordSuccess() {
        totalProcessed.increment();
        lastProcessedTime.set(System.currentTimeMillis());
    }

    /**
     * 처리 실패 기록
     */
    public void recordFailure() {
        totalFailed.increment();
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
