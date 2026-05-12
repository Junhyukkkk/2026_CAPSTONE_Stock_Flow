package com.stockflow.realtime.batch.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 배치 Job 실행 이력을 batch_job_runs 테이블에 기록.
 * Spring Batch 자체 메타 테이블(BATCH_JOB_EXECUTION)과 별개로
 * 비즈니스 관점의 실행 히스토리를 관리한다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchJobRunListener implements JobExecutionListener {

    private static final String CTX_RUN_ID = "batchRunId";

    private final JdbcTemplate jdbcTemplate;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        String jobName = jobExecution.getJobInstance().getJobName();
        try {
            Long runId = jdbcTemplate.queryForObject(
                    "INSERT INTO batch_job_runs (job_name, status) VALUES (?, 'RUNNING') RETURNING id",
                    Long.class, jobName
            );
            jobExecution.getExecutionContext().putLong(CTX_RUN_ID, runId != null ? runId : -1L);
            log.info("Batch job started: jobName={} runId={}", jobName, runId);
        } catch (Exception e) {
            log.warn("Failed to record batch_job_runs start: jobName={}", jobName, e);
        }
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long runId = jobExecution.getExecutionContext().getLong(CTX_RUN_ID, -1L);
        if (runId == -1L) return;

        String status = jobExecution.getStatus().isUnsuccessful() ? "FAILED" : "SUCCESS";
        String errorSummary = jobExecution.getAllFailureExceptions().isEmpty()
                ? null
                : jobExecution.getAllFailureExceptions().get(0).getMessage();

        long rowsWritten = jobExecution.getStepExecutions().stream()
                .mapToLong(s -> s.getWriteCount())
                .sum();

        try {
            jdbcTemplate.update(
                    "UPDATE batch_job_runs SET status=?, finished_at=NOW(), rows_written=?, error_summary=? WHERE id=?",
                    status, rowsWritten, errorSummary, runId
            );
            log.info("Batch job finished: status={} rowsWritten={} runId={}", status, rowsWritten, runId);
        } catch (Exception e) {
            log.warn("Failed to record batch_job_runs finish: runId={}", runId, e);
        }
    }
}
