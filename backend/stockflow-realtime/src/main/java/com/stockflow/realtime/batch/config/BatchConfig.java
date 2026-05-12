package com.stockflow.realtime.batch.config;

import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

/**
 * Spring Batch 기반 설정.
 * JobRepository·기본 JobLauncher는 Spring Boot 3.x 자동 구성이 처리.
 * 여기서는 API/수동 트리거용 비동기 런처만 추가 정의.
 */
@Configuration
public class BatchConfig {

    /**
     * 비동기 JobLauncher — REST API나 테스트에서 논블로킹 실행이 필요할 때 사용.
     * BatchScheduler의 스케줄 실행은 자동 구성된 동기 런처를 사용한다.
     */
    @Bean(name = "asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) throws Exception {
        TaskExecutorJobLauncher launcher = new TaskExecutorJobLauncher();
        launcher.setJobRepository(jobRepository);
        launcher.setTaskExecutor(new SimpleAsyncTaskExecutor("batch-async-"));
        launcher.afterPropertiesSet();
        return launcher;
    }
}
