package com.stockflow.realtime.batch.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 데이터 정합성 검증 Job.
 *
 * Step 1 — 갭(Gap) 검증:
 *   전일 데이터가 있는 심볼 중 당일 수집이 없는 심볼을 탐지.
 *   수집 파이프라인 장애 조기 감지에 활용.
 *
 * Step 2 — OHLCV 논리 검증:
 *   high < max(open, close) 또는 low > min(open, close)인 이상 레코드 탐지.
 *   Kafka 정규화 오류나 DB 적재 버그 검출.
 *
 * Step 3 — 급등락 이상 탐지:
 *   전일 대비 종가 변동률이 ±50% 초과인 심볼을 플래그.
 *   데이터 오류 또는 극단적 시장 이벤트 식별.
 *
 * Step 4 — 지표 적재 커버리지 검증:
 *   당일 OHLCV는 있으나 지표(symbol_daily_indicators)가 적재되지 않은 심볼을 탐지.
 *   지표 계산 Job이 일부 심볼을 누락했는지 조기 감지.
 *
 * Step 5 — 백테스트 입력 준비 상태 검증:
 *   암호화폐 일봉의 최소 학습 데이터(50일), 최신성, 중간 날짜 누락을 점검.
 *   데이터가 부족한 종목으로 백테스트를 실행하기 전에 운영자가 원인을 확인할 수 있게 한다.
 *
 * 각 Step의 결과는 batch_job_runs.meta(JSONB)에 저장된다.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class ValidationJobConfig {

    private final JobRepository              jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate              jdbcTemplate;
    private final BatchJobRunListener        jobRunListener;
    private final ObjectMapper               objectMapper;

    @Bean(name = "dataValidationJob")
    public Job dataValidationJob() {
        return new JobBuilder("dataValidationJob", jobRepository)
                .listener(jobRunListener)
                .start(gapValidationStep())
                .next(ohlcvSanityStep())
                .next(extremeMovementStep())
                .next(indicatorCoverageStep())
                .next(backtestReadinessStep())
                .build();
    }

    // ── Step 1: 갭 검증 ──────────────────────────────────────────────────────

    @Bean
    public Step gapValidationStep() {
        return new StepBuilder("gapValidationStep", jobRepository)
                .tasklet(gapValidationTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet gapValidationTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            LocalDate target   = LocalDate.parse(targetDate);
            LocalDate prevDate = target.minusDays(1);

            // 전일에는 있었지만 당일에는 없는 심볼
            List<String> missingSymbols = jdbcTemplate.queryForList(
                    """
                    SELECT prev.symbol
                    FROM symbol_daily_ohlcv prev
                    WHERE prev.trade_date = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM symbol_daily_ohlcv curr
                          WHERE curr.symbol = prev.symbol
                            AND curr.trade_date = ?
                      )
                    ORDER BY prev.symbol
                    """,
                    String.class,
                    Date.valueOf(prevDate), Date.valueOf(target)
            );

            if (missingSymbols.isEmpty()) {
                log.info("Gap validation PASS: no missing symbols for targetDate={}", target);
            } else {
                log.warn("Gap validation WARN: {} symbols missing on {} (were present on {}): {}",
                        missingSymbols.size(), target, prevDate, missingSymbols);
            }

            saveValidationResult("gap_check", targetDate, Map.of(
                    "targetDate",      targetDate,
                    "prevDate",        prevDate.toString(),
                    "missingCount",    missingSymbols.size(),
                    "missingSymbols",  missingSymbols
            ));

            return RepeatStatus.FINISHED;
        };
    }

    // ── Step 2: OHLCV 논리 검증 ──────────────────────────────────────────────

    @Bean
    public Step ohlcvSanityStep() {
        return new StepBuilder("ohlcvSanityStep", jobRepository)
                .tasklet(ohlcvSanityTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet ohlcvSanityTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            // high < max(open, close)  OR  low > min(open, close) 인 레코드
            List<Map<String, Object>> anomalies = jdbcTemplate.queryForList(
                    """
                    SELECT symbol, trade_date, open, high, low, close
                    FROM symbol_daily_ohlcv
                    WHERE trade_date = ?
                      AND (
                          high < GREATEST(open, close)
                       OR low  > LEAST(open, close)
                      )
                    ORDER BY symbol
                    """,
                    Date.valueOf(targetDate)
            );

            if (anomalies.isEmpty()) {
                log.info("OHLCV sanity PASS: all records valid for targetDate={}", targetDate);
            } else {
                log.warn("OHLCV sanity WARN: {} anomalous records on {}", anomalies.size(), targetDate);
                anomalies.forEach(r -> log.warn("  Anomaly: {}", r));
            }

            saveValidationResult("ohlcv_sanity", targetDate, Map.of(
                    "targetDate",    targetDate,
                    "anomalyCount",  anomalies.size(),
                    "anomalies",     anomalies
            ));

            return RepeatStatus.FINISHED;
        };
    }

    // ── Step 3: 급등락 이상 탐지 ─────────────────────────────────────────────

    @Bean
    public Step extremeMovementStep() {
        return new StepBuilder("extremeMovementStep", jobRepository)
                .tasklet(extremeMovementTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet extremeMovementTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            LocalDate target   = LocalDate.parse(targetDate);
            LocalDate prevDate = target.minusDays(1);

            // 전일 대비 종가 변동률 ±50% 초과
            List<Map<String, Object>> extremes = jdbcTemplate.queryForList(
                    """
                    SELECT
                        curr.symbol,
                        prev.close AS prev_close,
                        curr.close AS curr_close,
                        ROUND(((curr.close - prev.close) / prev.close * 100), 2) AS change_pct
                    FROM symbol_daily_ohlcv curr
                    JOIN symbol_daily_ohlcv prev
                      ON curr.symbol = prev.symbol
                     AND curr.source = prev.source
                    WHERE curr.trade_date = ?
                      AND prev.trade_date = ?
                      AND prev.close > 0
                      AND ABS((curr.close - prev.close) / prev.close) > 0.5
                    ORDER BY ABS((curr.close - prev.close) / prev.close) DESC
                    """,
                    Date.valueOf(target), Date.valueOf(prevDate)
            );

            if (extremes.isEmpty()) {
                log.info("Extreme movement PASS: no symbols with >50% move on {}", target);
            } else {
                log.warn("Extreme movement WARN: {} symbols with >50% price change on {}", extremes.size(), target);
                extremes.forEach(r -> log.warn("  Extreme: {}", r));
            }

            saveValidationResult("extreme_movement", targetDate, Map.of(
                    "targetDate",    targetDate,
                    "threshold",     "50%",
                    "extremeCount",  extremes.size(),
                    "extremes",      extremes
            ));

            return RepeatStatus.FINISHED;
        };
    }

    // ── Step 4: 지표 적재 커버리지 검증 ──────────────────────────────────────

    @Bean
    public Step indicatorCoverageStep() {
        return new StepBuilder("indicatorCoverageStep", jobRepository)
                .tasklet(indicatorCoverageTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet indicatorCoverageTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            // 당일 OHLCV 는 있으나 지표가 적재되지 않은 심볼
            List<String> missingIndicators = jdbcTemplate.queryForList(
                    """
                    SELECT o.symbol
                    FROM (SELECT DISTINCT symbol FROM symbol_daily_ohlcv WHERE trade_date = ?) o
                    WHERE NOT EXISTS (
                        SELECT 1 FROM symbol_daily_indicators i
                        WHERE i.symbol = o.symbol AND i.trade_date = ?
                    )
                    ORDER BY o.symbol
                    """,
                    String.class,
                    Date.valueOf(targetDate), Date.valueOf(targetDate)
            );

            if (missingIndicators.isEmpty()) {
                log.info("Indicator coverage PASS: all OHLCV symbols have indicators for {}", targetDate);
            } else {
                log.warn("Indicator coverage WARN: {} symbols missing indicators on {}: {}",
                        missingIndicators.size(), targetDate, missingIndicators);
            }

            saveValidationResult("indicator_coverage", targetDate, Map.of(
                    "targetDate",     targetDate,
                    "missingCount",   missingIndicators.size(),
                    "missingSymbols", missingIndicators
            ));

            return RepeatStatus.FINISHED;
        };
    }

    // ── Step 5: 백테스트 입력 준비 상태 검증 ─────────────────────────────────

    @Bean
    public Step backtestReadinessStep() {
        return new StepBuilder("backtestReadinessStep", jobRepository)
                .tasklet(backtestReadinessTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet backtestReadinessTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            LocalDate target = LocalDate.parse(targetDate);
            List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                    """
                    SELECT
                        symbol,
                        source,
                        COUNT(DISTINCT trade_date) AS observation_count,
                        MIN(trade_date) AS first_date,
                        MAX(trade_date) AS last_date,
                        GREATEST(?::date - MAX(trade_date), 0) AS stale_days,
                        GREATEST(
                            MAX(trade_date) - MIN(trade_date) + 1 - COUNT(DISTINCT trade_date),
                            0
                        ) AS internal_gap_days
                    FROM symbol_daily_ohlcv
                    WHERE market_type = 'CRYPTO'
                      AND trade_date <= ?::date
                    GROUP BY symbol, source
                    ORDER BY symbol, source
                    """,
                    Date.valueOf(target), Date.valueOf(target)
            );

            final int minimumHistoryDays = 50;
            int readyCount = 0;
            List<Map<String, Object>> attention = new ArrayList<>();

            for (Map<String, Object> row : rows) {
                int observations = ((Number) row.get("observation_count")).intValue();
                int staleDays = ((Number) row.get("stale_days")).intValue();
                int internalGapDays = ((Number) row.get("internal_gap_days")).intValue();
                boolean ready = observations >= minimumHistoryDays
                        && staleDays == 0
                        && internalGapDays == 0;

                if (ready) {
                    readyCount++;
                    continue;
                }

                Map<String, Object> issue = new LinkedHashMap<>();
                issue.put("symbol", row.get("symbol"));
                issue.put("source", row.get("source"));
                issue.put("observationCount", observations);
                issue.put("firstDate", row.get("first_date").toString());
                issue.put("lastDate", row.get("last_date").toString());
                issue.put("staleDays", staleDays);
                issue.put("internalGapDays", internalGapDays);
                issue.put("minimumHistoryDays", minimumHistoryDays);
                attention.add(issue);
            }

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("targetDate", targetDate);
            result.put("marketType", "CRYPTO");
            result.put("minimumHistoryDays", minimumHistoryDays);
            result.put("symbolSourceCount", rows.size());
            result.put("readyCount", readyCount);
            result.put("attentionCount", attention.size());
            result.put("attention", attention);
            saveValidationResult("backtest_readiness", targetDate, result);

            if (attention.isEmpty()) {
                log.info("Backtest readiness PASS: {} crypto symbol-source pairs ready on {}",
                        readyCount, target);
            } else {
                log.warn("Backtest readiness WARN: {}/{} crypto symbol-source pairs need attention on {}: {}",
                        attention.size(), rows.size(), target, attention);
            }

            return RepeatStatus.FINISHED;
        };
    }

    // ── 공통 유틸 ────────────────────────────────────────────────────────────

    private void saveValidationResult(String checkName, String targetDate, Map<String, Object> result) {
        try {
            Map<String, Object> meta = new HashMap<>();
            meta.put("checkName",  checkName);
            meta.put("targetDate", targetDate);
            meta.put("result",     result);

            String metaJson = objectMapper.writeValueAsString(meta);
            jdbcTemplate.update(
                    "INSERT INTO batch_job_runs (job_name, status, finished_at, meta) VALUES (?, 'SUCCESS', NOW(), ?::jsonb)",
                    "validation_" + checkName, metaJson
            );
        } catch (Exception e) {
            log.warn("Failed to save validation result: checkName={}", checkName, e);
        }
    }
}
