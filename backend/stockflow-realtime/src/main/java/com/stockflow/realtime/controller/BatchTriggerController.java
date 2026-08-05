package com.stockflow.realtime.controller;

import com.stockflow.realtime.batch.service.PrevCloseSyncService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 배치 수동 트리거 API
 *
 * 일일 배치는 매일 01:05 UTC에 자동 실행되지만, 시연·검증 시 즉시 실행이 필요할 때 사용한다.
 */
@Slf4j
@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchTriggerController {

    private final PrevCloseSyncService prevCloseSyncService;
    private final JobLauncher jobLauncher;

    @Qualifier("dailyOhlcvJob")
    private final Job dailyOhlcvJob;

    @Qualifier("dailyIndicatorJob")
    private final Job dailyIndicatorJob;

    @Qualifier("dataValidationJob")
    private final Job dataValidationJob;

    /**
     * 전일 종가 동기화를 즉시 실행한다.
     *
     * symbol_daily_ohlcv 의 심볼별 최신 종가를 Redis(price:prev-close:{symbol})에 적재한다.
     * 실행 후 실시간 시세의 등락률(changePercent)이 0이 아닌 값으로 계산되기 시작한다.
     *
     * @return 적재한 종목 수
     */
    @PostMapping("/prev-close-sync")
    public ResponseEntity<Map<String, Object>> syncPrevClose() {
        int loaded = prevCloseSyncService.syncFromDailyOhlcv();
        return ResponseEntity.ok(Map.of(
                "job", "prevCloseSync",
                "loadedSymbols", loaded,
                "message", loaded > 0
                        ? "전일 종가 적재 완료 — 실시간 등락률 계산이 활성화됩니다"
                        : "symbol_daily_ohlcv 에 일봉 데이터가 없습니다 (dailyOhlcvJob 선행 필요)"
        ));
    }

    /**
     * 일일 배치 전체를 지정 날짜로 즉시 실행한다 (OHLCV 집계 → 지표 계산 → 데이터 검증).
     *
     * 스케줄 실행과 달리 매번 고유 run.id 를 부여해 같은 날짜로도 재실행이 가능하다.
     *
     * @param date 대상 날짜 (YYYY-MM-DD, 생략 시 어제)
     * @return 각 Job 의 실행 상태
     */
    @PostMapping("/daily")
    public ResponseEntity<Map<String, Object>> runDailyBatch(
            @RequestParam(required = false) String date) {

        String targetDate = (date != null && !date.isBlank())
                ? date
                : LocalDate.now().minusDays(1).toString();

        Map<String, Object> results = new LinkedHashMap<>();
        results.put("targetDate", targetDate);

        // 스케줄 체인과 동일한 순서: OHLCV 집계 → 전일 종가 동기화 → 지표 계산 → 검증
        results.put("dailyOhlcvJob", runJob(dailyOhlcvJob, targetDate));
        results.put("prevCloseSync", prevCloseSyncService.syncFromDailyOhlcv() + " symbols");
        results.put("dailyIndicatorJob", runJob(dailyIndicatorJob, targetDate));
        results.put("dataValidationJob", runJob(dataValidationJob, targetDate));

        return ResponseEntity.ok(results);
    }

    private String runJob(Job job, String targetDate) {
        JobParameters params = new JobParametersBuilder()
                .addString("targetDate", targetDate)
                .addLong("run.id", System.nanoTime())   // 재실행 허용을 위한 고유 파라미터
                .toJobParameters();
        try {
            JobExecution exec = jobLauncher.run(job, params);
            return exec.getStatus().toString();
        } catch (Exception ex) {
            log.error("Manual batch job failed: job={} date={}", job.getName(), targetDate, ex);
            return "FAILED: " + ex.getMessage();
        }
    }
}
