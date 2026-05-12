package com.stockflow.realtime.batch.job;

import com.stockflow.realtime.batch.item.DailyOhlcvItem;
import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.DataClassRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;

/**
 * 일일 OHLCV 집계 Job.
 * market_ticks에서 전일 틱 데이터를 집계해 symbol_daily_ohlcv에 UPSERT한다.
 * TimescaleDB의 first()/last() 함수로 종가·시가를 정확하게 계산.
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class DailyOhlcvJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;
    private final BatchJobRunListener jobRunListener;

    private static final int CHUNK_SIZE = 200;

    @Bean(name = "dailyOhlcvJob")
    public Job dailyOhlcvJob() {
        return new JobBuilder("dailyOhlcvJob", jobRepository)
                .listener(jobRunListener)
                .start(ohlcvAggregationStep())
                .build();
    }

    @Bean
    public Step ohlcvAggregationStep() {
        return new StepBuilder("ohlcvAggregationStep", jobRepository)
                .<DailyOhlcvItem, DailyOhlcvItem>chunk(CHUNK_SIZE, transactionManager)
                .reader(ohlcvReader(null))
                .processor(ohlcvSanityProcessor())
                .writer(ohlcvWriter())
                .faultTolerant()
                .skipLimit(50)
                .skip(Exception.class)
                .build();
    }

    /**
     * market_ticks에서 targetDate 하루치를 심볼별로 OHLCV 집계.
     * TimescaleDB first(price, ts) / last(price, ts)로 시가·종가 결정.
     */
    @Bean
    @org.springframework.batch.core.configuration.annotation.StepScope
    public JdbcCursorItemReader<DailyOhlcvItem> ohlcvReader(
            @Value("#{jobParameters['targetDate']}") String targetDate) {

        String sql = """
                SELECT
                    symbol,
                    DATE(ts AT TIME ZONE 'UTC')  AS trade_date,
                    market_type,
                    source,
                    first(price, ts)             AS open,
                    MAX(price)                   AS high,
                    MIN(price)                   AS low,
                    last(price, ts)              AS close,
                    SUM(volume)                  AS volume,
                    COUNT(*)                     AS tick_count
                FROM market_ticks
                WHERE DATE(ts AT TIME ZONE 'UTC') = ?::date
                GROUP BY symbol, DATE(ts AT TIME ZONE 'UTC'), market_type, source
                ORDER BY symbol
                """;

        return new JdbcCursorItemReaderBuilder<DailyOhlcvItem>()
                .name("ohlcvReader")
                .dataSource(dataSource)
                .sql(sql)
                .preparedStatementSetter(ps -> ps.setDate(1, Date.valueOf(targetDate)))
                .rowMapper((rs, rowNum) -> DailyOhlcvItem.builder()
                        .symbol(rs.getString("symbol"))
                        .tradeDate(rs.getDate("trade_date").toLocalDate())
                        .marketType(rs.getString("market_type"))
                        .source(rs.getString("source"))
                        .open(rs.getBigDecimal("open"))
                        .high(rs.getBigDecimal("high"))
                        .low(rs.getBigDecimal("low"))
                        .close(rs.getBigDecimal("close"))
                        .volume(rs.getBigDecimal("volume"))
                        .tickCount(rs.getLong("tick_count"))
                        .build())
                .build();
    }

    /**
     * OHLCV 기본 무결성 검증: high >= max(open,close), low <= min(open,close).
     * 위반 항목은 스킵하고 경고 로그 남김.
     */
    @Bean
    public ItemProcessor<DailyOhlcvItem, DailyOhlcvItem> ohlcvSanityProcessor() {
        return item -> {
            BigDecimal expectedHigh = item.getOpen().max(item.getClose());
            BigDecimal expectedLow  = item.getOpen().min(item.getClose());

            if (item.getHigh().compareTo(expectedHigh) < 0
                    || item.getLow().compareTo(expectedLow) > 0) {
                log.warn("OHLCV sanity fail — skipping: symbol={} date={} O={} H={} L={} C={}",
                        item.getSymbol(), item.getTradeDate(),
                        item.getOpen(), item.getHigh(), item.getLow(), item.getClose());
                return null; // ItemProcessor에서 null 반환 시 해당 아이템 필터링
            }
            return item;
        };
    }

    /**
     * symbol_daily_ohlcv에 UPSERT (기존 레코드가 있으면 갱신).
     */
    @Bean
    public JdbcBatchItemWriter<DailyOhlcvItem> ohlcvWriter() {
        String sql = """
                INSERT INTO symbol_daily_ohlcv
                    (symbol, trade_date, market_type, source, open, high, low, close, volume, tick_count, computed_at)
                VALUES (:symbol, :tradeDate, :marketType, :source, :open, :high, :low, :close, :volume, :tickCount, NOW())
                ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
                    market_type  = EXCLUDED.market_type,
                    open         = EXCLUDED.open,
                    high         = EXCLUDED.high,
                    low          = EXCLUDED.low,
                    close        = EXCLUDED.close,
                    volume       = EXCLUDED.volume,
                    tick_count   = EXCLUDED.tick_count,
                    computed_at  = NOW()
                """;

        return new JdbcBatchItemWriterBuilder<DailyOhlcvItem>()
                .dataSource(dataSource)
                .sql(sql)
                .beanMapped()
                .build();
    }
}
