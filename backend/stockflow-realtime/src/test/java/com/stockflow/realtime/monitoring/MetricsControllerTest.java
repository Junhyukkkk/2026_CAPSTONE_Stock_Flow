package com.stockflow.realtime.monitoring;

import com.stockflow.core.metrics.PerformanceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class MetricsControllerTest {

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        PerformanceMetrics metrics = new PerformanceMetrics(new SimpleMeterRegistry());
        metrics.recordSuccess();
        metrics.recordFailure();
        ConsumerLagMonitor lagMonitor = mock(ConsumerLagMonitor.class);
        when(lagMonitor.getConsumerLag("rt")).thenReturn(Map.of(0, 7L));
        when(lagMonitor.getTotalLag("rt")).thenReturn(7L);
        when(lagMonitor.getConsumerLag("st")).thenReturn(Map.of());
        when(lagMonitor.getTotalLag("st")).thenReturn(0L);
        MetricsController controller = new MetricsController(metrics, lagMonitor);
        ReflectionTestUtils.setField(controller, "realtimeGroup", "rt");
        ReflectionTestUtils.setField(controller, "storageGroup", "st");
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void performanceEndpointReportsCounters() throws Exception {
        mvc.perform(get("/api/metrics/performance")).andExpect(status().isOk())
                .andExpect(jsonPath("$.totalProcessed").value(1))
                .andExpect(jsonPath("$.totalFailed").value(1))
                .andExpect(jsonPath("$.minProcessingTime").value(0));
    }

    @Test
    void consumerLagEndpointReportsBothGroups() throws Exception {
        mvc.perform(get("/api/metrics/consumer-lag")).andExpect(status().isOk())
                .andExpect(jsonPath("$.realtimeGroup.groupId").value("rt"))
                .andExpect(jsonPath("$.realtimeGroup.totalLag").value(7))
                .andExpect(jsonPath("$.storageGroup.totalLag").value(0));
    }

    @Test
    void rootEndpointAggregatesEverything() throws Exception {
        mvc.perform(get("/api/metrics")).andExpect(status().isOk())
                .andExpect(jsonPath("$.performance.totalProcessed").value(1))
                .andExpect(jsonPath("$.consumerLag.realtimeGroup.groupId").value("rt"));
    }
}
