package com.stockflow.realtime.test;

import com.stockflow.core.metrics.PerformanceMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 성능 메트릭 조회/리셋용 테스트 컨트롤러
 *
 * test 프로파일에서만 활성화
 */
@Slf4j
@RestController
@RequestMapping("/test/metrics")
@Profile("test")
@RequiredArgsConstructor
public class TestMetricsController {

    private final PerformanceMetrics performanceMetrics;

    @GetMapping
    public Map<String, Object> getMetrics() {
        log.info("Fetching performance metrics");
        return performanceMetrics.getMetricsSnapshot();
    }

    @PostMapping("/reset")
    public Map<String, Object> resetMetrics() {
        log.info("Resetting performance metrics");
        performanceMetrics.reset();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "success");
        response.put("message", "Metrics have been reset");
        response.put("resetAt", System.currentTimeMillis());

        return response;
    }
}
