package com.stockflow.realtime.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/** 대량 모델 실행이 웹 요청 처리 스레드를 점유하지 않도록 전용 단일 작업 큐를 둔다. */
@Configuration
public class PerformanceReportJobConfig {

    @Bean(name = "performanceReportTaskExecutor")
    public ThreadPoolTaskExecutor performanceReportTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(1);
        executor.setQueueCapacity(1);
        executor.setThreadNamePrefix("performance-report-");
        executor.initialize();
        return executor;
    }
}
