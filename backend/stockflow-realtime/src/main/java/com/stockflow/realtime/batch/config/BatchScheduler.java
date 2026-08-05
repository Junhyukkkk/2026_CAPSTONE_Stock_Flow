package com.stockflow.realtime.batch.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * 일일 배치 Job 스케줄러.
 * 매일 01:05 UTC에 OHLCV 집계 → 지표 계산 → 데이터 검증 순으로 실행.
 * 자동 구성된 동기 JobLauncher를 사용해 순서를 보장한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchScheduler {

    private final JobLauncher jobLauncher;

    @Qualifier("dailyOhlcvJob")
    private final Job dailyOhlcvJob;

    @Qualifier("prevCloseSyncJob")
    private final Job prevCloseSyncJob;

    @Qualifier("dailyIndicatorJob")
    private final Job dailyIndicatorJob;

    @Qualifier("dataValidationJob")
    private final Job dataValidationJob;

    // 매일 01:05 UTC (데이터 수집이 안정화된 이후)
    @Scheduled(cron = "${batch.schedule.cron:0 5 1 * * *}", zone = "UTC")
    public void runDailyBatch() {
        String targetDate = LocalDate.now().minusDays(1).toString(); // 전일 데이터 처리
        log.info("Daily batch starting for targetDate={}", targetDate);

        runJob(dailyOhlcvJob, "dailyOhlcvJob", targetDate);
        // 일봉 집계 직후: 갓 마감된 종가를 실시간 등락률 계산용 전일 종가로 Redis에 적재
        runJob(prevCloseSyncJob, "prevCloseSyncJob", targetDate);
        runJob(dailyIndicatorJob, "dailyIndicatorJob", targetDate);
        runJob(dataValidationJob, "dataValidationJob", targetDate);

        log.info("Daily batch complete for targetDate={}", targetDate);
    }

    private void runJob(Job job, String jobName, String targetDate) {
        JobParameters params = new JobParametersBuilder()
                .addString("targetDate", targetDate)
                .toJobParameters();
        try {
            jobLauncher.run(job, params);
        } catch (Exception e) {
            log.error("Batch job failed: jobName={} targetDate={}", jobName, targetDate, e);
        }
    }
}
