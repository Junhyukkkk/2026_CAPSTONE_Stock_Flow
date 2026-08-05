package com.stockflow.realtime.batch.job;

import com.stockflow.realtime.batch.item.DailyIndicatorItem;
import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import com.stockflow.realtime.batch.service.OhlcvData;
import com.stockflow.realtime.batch.service.TechnicalIndicatorService;
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

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * 일일 기술적 지표 계산 Job.
 * symbol_daily_ohlcv에서 심볼별 최근 60일 종가를 읽어
 * MA5/MA20/MA60/RSI14/MACD를 계산한 뒤 symbol_daily_indicators에 UPSERT.
 *
 * Tasklet 방식을 선택한 이유:
 * 지표 계산은 심볼 하나당 60개 행의 히스토리가 필요해
 * 청크 기반 Reader/Processor 패턴보다 Tasklet이 더 적합하다.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class IndicatorJobConfig {

    private final JobRepository              jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate              jdbcTemplate;
    private final TechnicalIndicatorService  indicatorService;
    private final BatchJobRunListener        jobRunListener;

    /** 지표 계산에 필요한 최소 이력 일수 (MACD 기준) */
    private static final int HISTORY_DAYS = 60;

    @Bean(name = "dailyIndicatorJob")
    public Job dailyIndicatorJob() {
        return new JobBuilder("dailyIndicatorJob", jobRepository)
                .listener(jobRunListener)
                .start(indicatorCalculationStep())
                .build();
    }

    @Bean
    public Step indicatorCalculationStep() {
        return new StepBuilder("indicatorCalculationStep", jobRepository)
                .tasklet(indicatorCalculationTasklet(null), transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet indicatorCalculationTasklet(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        return (contribution, chunkContext) -> {
            LocalDate target = LocalDate.parse(targetDate);

            // 1. targetDate에 OHLCV 데이터가 있는 심볼 목록 조회
            List<String> symbols = jdbcTemplate.queryForList(
                    "SELECT DISTINCT symbol FROM symbol_daily_ohlcv WHERE trade_date = ?",
                    String.class, Date.valueOf(target)
            );

            log.info("Indicator calculation started: targetDate={} symbols={}", target, symbols.size());

            List<DailyIndicatorItem> batch = new ArrayList<>();
            int computed = 0;
            int failed = 0;

            for (String symbol : symbols) {
                try {
                    // 2. 심볼별 "최신" HISTORY_DAYS일 OHLCV 조회 (오래된 순으로 정렬해 반환)
                    //    - 서브쿼리에서 DESC LIMIT 으로 targetDate 이하 최근 N일을 뽑고
                    //      바깥에서 ASC 로 뒤집는다. (예전엔 ASC LIMIT 이라 가장 오래된 N일을 가져와
                    //      이력이 N일을 넘으면 지표가 옛 데이터로 고정 계산되던 버그)
                    //    - DISTINCT ON (trade_date) 로 한 심볼이 여러 source 를 가져도
                    //      날짜당 한 행만 사용해 시계열이 중복되지 않게 한다.
                    List<OhlcvData> ohlcvList = jdbcTemplate.query(
                            """
                            SELECT open, high, low, close, volume
                            FROM (
                                SELECT DISTINCT ON (trade_date)
                                       trade_date, open, high, low, close, volume
                                FROM symbol_daily_ohlcv
                                WHERE symbol = ?
                                  AND trade_date <= ?
                                ORDER BY trade_date DESC, source
                                LIMIT ?
                            ) recent
                            ORDER BY trade_date ASC
                            """,
                            (rs, rowNum) -> new OhlcvData(
                                    rs.getBigDecimal("open"),
                                    rs.getBigDecimal("high"),
                                    rs.getBigDecimal("low"),
                                    rs.getBigDecimal("close"),
                                    rs.getBigDecimal("volume")
                            ),
                            symbol, Date.valueOf(target), HISTORY_DAYS
                    );

                    if (ohlcvList.isEmpty()) continue;

                    // 3. 지표 계산 (OHLCV 기반)
                    DailyIndicatorItem item = indicatorService.computeWithOhlcv(symbol, target, ohlcvList);
                    batch.add(item);
                    computed++;

                    // 4. 100건마다 중간 UPSERT (메모리 절약)
                    if (batch.size() >= 100) {
                        upsertIndicators(batch);
                        batch.clear();
                    }
                } catch (Exception e) {
                    // 한 심볼의 실패가 나머지 심볼의 지표 적재까지 막지 않도록 격리한다.
                    failed++;
                    log.warn("Indicator calc skipped for symbol={} on {}: {}", symbol, target, e.toString());
                }
            }

            // 5. 나머지 잔여분 UPSERT
            if (!batch.isEmpty()) {
                upsertIndicators(batch);
            }

            contribution.incrementWriteCount(computed);
            log.info("Indicator calculation finished: targetDate={} symbols={} computed={} failed={}",
                    target, symbols.size(), computed, failed);
            return RepeatStatus.FINISHED;
        };
    }

    private void upsertIndicators(List<DailyIndicatorItem> items) {
        String sql = """
                INSERT INTO symbol_daily_indicators
                    (symbol, trade_date, ma5, ma20, ma60, rsi14, macd, macd_signal, macd_hist,
                     bb_upper, bb_lower, stoch_k, stoch_d, atr14, obv, computed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    ma5         = EXCLUDED.ma5,
                    ma20        = EXCLUDED.ma20,
                    ma60        = EXCLUDED.ma60,
                    rsi14       = EXCLUDED.rsi14,
                    macd        = EXCLUDED.macd,
                    macd_signal = EXCLUDED.macd_signal,
                    macd_hist   = EXCLUDED.macd_hist,
                    bb_upper    = EXCLUDED.bb_upper,
                    bb_lower    = EXCLUDED.bb_lower,
                    stoch_k     = EXCLUDED.stoch_k,
                    stoch_d     = EXCLUDED.stoch_d,
                    atr14       = EXCLUDED.atr14,
                    obv         = EXCLUDED.obv,
                    computed_at = NOW()
                """;

        jdbcTemplate.batchUpdate(sql, items, items.size(), (ps, item) -> {
            ps.setString(1, item.getSymbol());
            ps.setDate(2, Date.valueOf(item.getTradeDate()));
            ps.setBigDecimal(3, item.getMa5());
            ps.setBigDecimal(4, item.getMa20());
            ps.setBigDecimal(5, item.getMa60());
            ps.setBigDecimal(6, item.getRsi14());
            ps.setBigDecimal(7, item.getMacd());
            ps.setBigDecimal(8, item.getMacdSignal());
            ps.setBigDecimal(9, item.getMacdHist());
            ps.setBigDecimal(10, item.getBbUpper());
            ps.setBigDecimal(11, item.getBbLower());
            ps.setBigDecimal(12, item.getStochK());
            ps.setBigDecimal(13, item.getStochD());
            ps.setBigDecimal(14, item.getAtr14());
            if (item.getObv() != null) {
                ps.setLong(15, item.getObv());
            } else {
                ps.setNull(15, java.sql.Types.BIGINT);
            }
        });
    }
}
