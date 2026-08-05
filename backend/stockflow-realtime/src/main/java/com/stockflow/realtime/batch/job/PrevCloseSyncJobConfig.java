package com.stockflow.realtime.batch.job;

import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import com.stockflow.realtime.batch.service.PrevCloseSyncService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * 전일 종가 동기화 Job.
 *
 * symbol_daily_ohlcv 의 심볼별 최신 종가를 Redis(price:prev-close:{symbol})에 적재한다.
 * 단일 Tasklet — 심볼 수가 수백 개 규모라 청크 처리가 필요 없다.
 * dailyOhlcvJob 직후에 실행되어야 갓 마감된 종가가 반영된다.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PrevCloseSyncJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchJobRunListener jobRunListener;
    private final PrevCloseSyncService prevCloseSyncService;

    @Bean(name = "prevCloseSyncJob")
    public Job prevCloseSyncJob() {
        return new JobBuilder("prevCloseSyncJob", jobRepository)
                .listener(jobRunListener)
                .start(prevCloseSyncStep())
                .build();
    }

    @Bean
    public Step prevCloseSyncStep() {
        return new StepBuilder("prevCloseSyncStep", jobRepository)
                .tasklet(prevCloseSyncTasklet(), transactionManager)
                .build();
    }

    @Bean
    public Tasklet prevCloseSyncTasklet() {
        return (contribution, chunkContext) -> {
            int loaded = prevCloseSyncService.syncFromDailyOhlcv();
            contribution.incrementWriteCount(loaded);
            return RepeatStatus.FINISHED;
        };
    }
}
