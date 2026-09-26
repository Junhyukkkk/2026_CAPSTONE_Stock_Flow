package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.PerformanceReportJobRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportJobResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.PerformanceReportUniverseResponse;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.PerformanceReportJobRepository;
import com.stockflow.realtime.backtest.repository.PerformanceReportJobRepository.JobRow;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 전체 암호화폐 성과 리포트를 한 번에 하나씩 비동기로 실행한다. */
@Service
public class PerformanceReportJobService {

    private static final BigDecimal DEFAULT_INITIAL_CASH = BigDecimal.valueOf(10000);

    private final BacktestRunService runService;
    private final PerformanceReportJobRepository jobRepository;
    private final TaskExecutor taskExecutor;

    public PerformanceReportJobService(
            BacktestRunService runService,
            PerformanceReportJobRepository jobRepository,
            @Qualifier("performanceReportTaskExecutor") TaskExecutor taskExecutor) {
        this.runService = runService;
        this.jobRepository = jobRepository;
        this.taskExecutor = taskExecutor;
    }

    public PerformanceReportJobResponse start(PerformanceReportJobRequest request) {
        LocalDate from = request.getFrom();
        LocalDate to = request.getTo();
        int minimumHistoryDays = request.getMinimumHistoryDays() == null ? 50 : request.getMinimumHistoryDays();
        BigDecimal initialCash = request.getInitialCash() == null ? DEFAULT_INITIAL_CASH : request.getInitialCash();
        if (initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }
        if (jobRepository.hasActiveJob()) {
            throw new IllegalStateException("An all-crypto performance report is already running");
        }

        PerformanceReportUniverseResponse universe = runService.inspectPerformanceReportUniverse(
                from, to, minimumHistoryDays);
        if (universe.eligibleSymbols().isEmpty()) {
            throw new IllegalArgumentException("No eligible crypto symbols for the selected range and history requirement");
        }
        long jobId = jobRepository.createJob(from, to, initialCash, minimumHistoryDays,
                universe.eligibleSymbols().size());
        taskExecutor.execute(() -> run(jobId, universe.eligibleSymbols(), from, to, initialCash));
        return get(jobId).orElseThrow();
    }

    public Optional<PerformanceReportJobResponse> get(long jobId) {
        return jobRepository.findJob(jobId).map(job -> toResponse(job, jobRepository.findItems(jobId)));
    }

    private void run(long jobId, List<String> symbols, LocalDate from, LocalDate to, BigDecimal initialCash) {
        try {
            jobRepository.markRunning(jobId);
            for (String symbol : symbols) {
                List<PerformanceReportRow> rows = new ArrayList<>(4);
                rows.add(runService.runPerformanceReportRow(
                        symbol, StrategyType.BUY_AND_HOLD, null, Map.of(), initialCash, from, to));
                for (String model : runService.reportModels()) {
                    rows.add(runService.runPerformanceReportRow(
                            symbol, StrategyType.PREDICTION, model,
                            runService.defaultReportPredictionParams(model), initialCash, from, to));
                }
                for (PerformanceReportRow row : rows) {
                    jobRepository.saveItem(jobId, row);
                }
                int success = (int) rows.stream().filter(row -> "SUCCESS".equals(row.status())).count();
                jobRepository.markSymbolComplete(jobId, success, rows.size() - success);
            }
            jobRepository.markSucceeded(jobId);
        } catch (RuntimeException e) {
            jobRepository.markFailed(jobId, e.getMessage());
        }
    }

    private PerformanceReportJobResponse toResponse(JobRow job, List<PerformanceReportRow> rows) {
        return new PerformanceReportJobResponse(
                job.id(), job.status(), job.fromDate(), job.toDate(), job.initialCash(),
                job.minimumHistoryDays(), job.totalSymbols(), job.completedSymbols(),
                job.successfulRows(), job.failedRows(), job.createdAt(), job.startedAt(), job.finishedAt(),
                job.errorSummary(), rows, runService.summarizePerformanceReportRows(rows));
    }
}
