package com.stockflow.realtime.batch.config;

import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BatchInfrastructureTest {

    // ---------- BatchConfig ----------

    @Test
    void asyncJobLauncherIsBuilt() throws Exception {
        JobLauncher launcher = new BatchConfig().asyncJobLauncher(mock(JobRepository.class));

        assertThat(launcher).isNotNull();
    }

    // ---------- BatchScheduler ----------

    @Test
    void schedulerRunsAllFourJobsInOrderWithTargetDate() throws Exception {
        JobLauncher launcher = mock(JobLauncher.class);
        Job ohlcv = mock(Job.class);
        Job prevClose = mock(Job.class);
        Job indicator = mock(Job.class);
        Job validation = mock(Job.class);

        new BatchScheduler(launcher, ohlcv, prevClose, indicator, validation).runDailyBatch();

        var order = inOrder(launcher);
        order.verify(launcher).run(eq(ohlcv), any(JobParameters.class));
        order.verify(launcher).run(eq(prevClose), any(JobParameters.class));
        order.verify(launcher).run(eq(indicator), any(JobParameters.class));
        order.verify(launcher).run(eq(validation), any(JobParameters.class));
    }

    @Test
    void schedulerKeepsGoingWhenAJobFails() throws Exception {
        JobLauncher launcher = mock(JobLauncher.class);
        Job failing = mock(Job.class);
        Job other = mock(Job.class);
        when(launcher.run(eq(failing), any(JobParameters.class))).thenThrow(new IllegalStateException("locked"));

        new BatchScheduler(launcher, failing, other, other, other).runDailyBatch();

        verify(launcher, times(3)).run(eq(other), any(JobParameters.class));
    }

    // ---------- BatchJobRunListener ----------

    private static JobExecution execution(BatchStatus status) {
        JobExecution execution = new JobExecution(new JobInstance(1L, "dailyOhlcvJob"), new JobParameters());
        execution.setStatus(status);
        return execution;
    }

    @Test
    void listenerRecordsStartAndSuccessfulFinish() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), eq("dailyOhlcvJob"))).thenReturn(42L);
        BatchJobRunListener listener = new BatchJobRunListener(jdbc);
        JobExecution execution = execution(BatchStatus.COMPLETED);
        StepExecution step = execution.createStepExecution("step");
        step.setWriteCount(7);

        listener.beforeJob(execution);
        listener.afterJob(execution);

        verify(jdbc).update(contains("UPDATE batch_job_runs"), eq("SUCCESS"), eq(7L), eq((String) null), eq(42L));
    }

    @Test
    void listenerRecordsFailureWithFirstExceptionMessage() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), anyString())).thenReturn(5L);
        BatchJobRunListener listener = new BatchJobRunListener(jdbc);
        JobExecution execution = execution(BatchStatus.FAILED);
        execution.addFailureException(new RuntimeException("db exploded"));

        listener.beforeJob(execution);
        listener.afterJob(execution);

        verify(jdbc).update(contains("UPDATE batch_job_runs"), eq("FAILED"), eq(0L), eq("db exploded"), eq(5L));
    }

    @Test
    void listenerToleratesDatabaseErrors() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), anyString())).thenThrow(new IllegalStateException("db down"));
        BatchJobRunListener listener = new BatchJobRunListener(jdbc);
        JobExecution execution = execution(BatchStatus.COMPLETED);

        listener.beforeJob(execution);   // 시작 기록 실패 → runId 없음
        listener.afterJob(execution);    // runId 가 없으면 조용히 종료

        verify(jdbc, never()).update(anyString(), any(Object[].class));
    }

    @Test
    void listenerSurvivesFailingFinishUpdate() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class), anyString())).thenReturn(null);
        when(jdbc.update(anyString(), any(Object[].class))).thenThrow(new IllegalStateException("db down"));
        BatchJobRunListener listener = new BatchJobRunListener(jdbc);
        JobExecution execution = execution(BatchStatus.COMPLETED);

        listener.beforeJob(execution);   // runId null → -1 저장 → afterJob 에서 건너뜀
        listener.afterJob(execution);
        verify(jdbc, never()).update(anyString(), any(Object[].class));

        execution.getExecutionContext().putLong("batchRunId", 9L);
        listener.afterJob(execution);    // update 가 예외를 던져도 전파되지 않아야 한다
    }
}
