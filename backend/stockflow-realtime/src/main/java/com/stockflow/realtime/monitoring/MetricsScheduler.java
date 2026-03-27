package com.stockflow.realtime.monitoring;

import com.stockflow.core.metrics.PerformanceMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 메트릭 스케줄러
 * 
 * 주기적으로 메트릭을 로깅
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsScheduler {

    private final PerformanceMetrics performanceMetrics;
    private final ConsumerLagMonitor consumerLagMonitor;

    @Value("${spring.kafka.consumer.group.realtime:realtime-group}")
    private String realtimeGroup;

    @Value("${spring.kafka.consumer.group.storage:storage-group}")
    private String storageGroup;

    /**
     * 1분마다 메트릭 로깅
     */
    @Scheduled(fixedRate = 60000) // 1분
    public void logMetrics() {
        // 성능 메트릭 로깅
        performanceMetrics.logMetrics();
        
        // Consumer Lag 로깅
        long realtimeLag = consumerLagMonitor.getTotalLag(realtimeGroup);
        long storageLag = consumerLagMonitor.getTotalLag(storageGroup);
        
        if (realtimeLag > 0 || storageLag > 0) {
            log.warn("Consumer Lag detected - Realtime: {}, Storage: {}", 
                realtimeLag, storageLag);
        } else {
            log.debug("Consumer Lag - Realtime: {}, Storage: {}", 
                realtimeLag, storageLag);
        }
    }
}
