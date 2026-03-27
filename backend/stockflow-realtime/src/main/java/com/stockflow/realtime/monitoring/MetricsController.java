package com.stockflow.realtime.monitoring;

import com.stockflow.core.metrics.PerformanceMetrics;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 메트릭 조회 API
 * 
 * 성능 메트릭 및 Consumer 상태 조회
 */
@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final PerformanceMetrics performanceMetrics;
    private final ConsumerLagMonitor consumerLagMonitor;

    @Value("${spring.kafka.consumer.group.realtime:realtime-group}")
    private String realtimeGroup;

    @Value("${spring.kafka.consumer.group.storage:storage-group}")
    private String storageGroup;

    /**
     * 성능 메트릭 조회
     */
    @GetMapping("/performance")
    public ResponseEntity<Map<String, Object>> getPerformanceMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        
        metrics.put("throughputPerSecond", performanceMetrics.getThroughputPerSecond());
        metrics.put("averageProcessingTime", performanceMetrics.getAverageProcessingTime());
        metrics.put("minProcessingTime", performanceMetrics.getMinProcessingTime().get() == Long.MAX_VALUE ? 
            0 : performanceMetrics.getMinProcessingTime().get());
        metrics.put("maxProcessingTime", performanceMetrics.getMaxProcessingTime().get());
        metrics.put("errorRate", performanceMetrics.getErrorRate());
        metrics.put("totalProcessed", performanceMetrics.getTotalProcessed().sum());
        metrics.put("totalFailed", performanceMetrics.getTotalFailed().sum());
        
        return ResponseEntity.ok(metrics);
    }

    /**
     * Consumer Lag 조회
     */
    @GetMapping("/consumer-lag")
    public ResponseEntity<Map<String, Object>> getConsumerLag() {
        Map<String, Object> lagInfo = new HashMap<>();
        
        Map<Integer, Long> realtimeLag = consumerLagMonitor.getConsumerLag(realtimeGroup);
        Map<Integer, Long> storageLag = consumerLagMonitor.getConsumerLag(storageGroup);
        
        Map<String, Object> realtimeInfo = new HashMap<>();
        realtimeInfo.put("groupId", realtimeGroup);
        realtimeInfo.put("lagByPartition", realtimeLag);
        realtimeInfo.put("totalLag", consumerLagMonitor.getTotalLag(realtimeGroup));
        
        Map<String, Object> storageInfo = new HashMap<>();
        storageInfo.put("groupId", storageGroup);
        storageInfo.put("lagByPartition", storageLag);
        storageInfo.put("totalLag", consumerLagMonitor.getTotalLag(storageGroup));
        
        lagInfo.put("realtimeGroup", realtimeInfo);
        lagInfo.put("storageGroup", storageInfo);
        
        return ResponseEntity.ok(lagInfo);
    }

    /**
     * 전체 메트릭 조회
     */
    @GetMapping
    public ResponseEntity<Map<String, Object>> getAllMetrics() {
        Map<String, Object> allMetrics = new HashMap<>();
        
        allMetrics.put("performance", getPerformanceMetrics().getBody());
        allMetrics.put("consumerLag", getConsumerLag().getBody());
        
        return ResponseEntity.ok(allMetrics);
    }
}
