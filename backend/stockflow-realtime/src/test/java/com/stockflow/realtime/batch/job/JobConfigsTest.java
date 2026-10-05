package com.stockflow.realtime.batch.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.realtime.batch.item.DailyIndicatorItem;
import com.stockflow.realtime.batch.item.DailyOhlcvItem;
import com.stockflow.realtime.batch.listener.BatchJobRunListener;
import com.stockflow.realtime.batch.service.OhlcvData;
import com.stockflow.realtime.batch.service.PrevCloseSyncService;
import com.stockflow.realtime.batch.service.TechnicalIndicatorService;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class JobConfigsTest {

    JobRepository jobRepository = mock(JobRepository.class);
    PlatformTransactionManager txManager = mock(PlatformTransactionManager.class);
    BatchJobRunListener listener = mock(BatchJobRunListener.class);
    JdbcTemplate jdbc = mock(JdbcTemplate.class);

    private static StepContribution contribution() {
        return new StepExecution("step", new JobExecution(1L)).createStepContribution();
    }

    // ---------- DailyOhlcvJobConfig ----------

    private static DailyOhlcvItem ohlcv(String open, String high, String low, String close) {
        return DailyOhlcvItem.builder().symbol("BTC").tradeDate(LocalDate.of(2025, 1, 1))
                .open(new BigDecimal(open)).high(new BigDecimal(high)).low(new BigDecimal(low))
                .close(new BigDecimal(close)).build();
    }

    @Test
    void dailyOhlcvJobWiringBuilds() {
        DailyOhlcvJobConfig config = new DailyOhlcvJobConfig(jobRepository, txManager, mock(DataSource.class), listener);

        Job job = config.dailyOhlcvJob();

        assertThat(job.getName()).isEqualTo("dailyOhlcvJob");
        assertThat(config.ohlcvAggregationStep().getName()).isEqualTo("ohlcvAggregationStep");
        assertThat(config.ohlcvWriter()).isNotNull();
    }

    @Test
    void ohlcvSanityProcessorFiltersInconsistentCandles() throws Exception {
        ItemProcessor<DailyOhlcvItem, DailyOhlcvItem> processor =
                new DailyOhlcvJobConfig(jobRepository, txManager, mock(DataSource.class), listener).ohlcvSanityProcessor();

        DailyOhlcvItem valid = ohlcv("10", "12", "9", "11");
        assertThat(processor.process(valid)).isSameAs(valid);
        assertThat(processor.process(ohlcv("10", "10", "9", "11"))).isNull();  // high < close
        assertThat(processor.process(ohlcv("10", "12", "10.5", "11"))).isNull(); // low > open
    }

    @Test
    void ohlcvReaderBindsTargetDateAndMapsRows() throws Exception {
        DailyOhlcvJobConfig config = new DailyOhlcvJobConfig(jobRepository, txManager, mock(DataSource.class), listener);

        JdbcCursorItemReader<DailyOhlcvItem> reader = config.ohlcvReader("2025-01-01");

        PreparedStatementSetter setter = (PreparedStatementSetter) ReflectionTestUtils.getField(reader, "preparedStatementSetter");
        PreparedStatement ps = mock(PreparedStatement.class);
        setter.setValues(ps);
        verify(ps).setDate(1, java.sql.Date.valueOf("2025-01-01"));

        RowMapper<DailyOhlcvItem> mapper = (RowMapper<DailyOhlcvItem>) ReflectionTestUtils.getField(reader, "rowMapper");
        Map<String, Object> row = Map.of("symbol", "BTC", "trade_date", LocalDate.of(2025, 1, 1), "market_type", "CRYPTO",
                "source", "BINANCE", "open", "1", "high", "2", "low", "0.5", "close", "1.5", "volume", "9", "tick_count", 3L);
        DailyOhlcvItem item = mapper.mapRow(JdbcTestSupport.resultSet(row), 0);
        assertThat(item.getSymbol()).isEqualTo("BTC");
        assertThat(item.getTickCount()).isEqualTo(3L);
        assertThat(item.getClose()).isEqualByComparingTo("1.5");
    }

    // ---------- PrevCloseSyncJobConfig ----------

    @Test
    void prevCloseSyncJobRunsTheSyncService() throws Exception {
        PrevCloseSyncService service = mock(PrevCloseSyncService.class);
        when(service.syncFromDailyOhlcv()).thenReturn(4);
        PrevCloseSyncJobConfig config = new PrevCloseSyncJobConfig(jobRepository, txManager, listener, service);

        assertThat(config.prevCloseSyncJob().getName()).isEqualTo("prevCloseSyncJob");
        assertThat(config.prevCloseSyncStep().getName()).isEqualTo("prevCloseSyncStep");
        StepContribution contribution = contribution();

        assertThat(config.prevCloseSyncTasklet().execute(contribution, null)).isEqualTo(RepeatStatus.FINISHED);
        assertThat(contribution.getWriteCount()).isEqualTo(4);
    }

    // ---------- IndicatorJobConfig ----------

    private IndicatorJobConfig indicatorConfig(TechnicalIndicatorService service) {
        return new IndicatorJobConfig(jobRepository, txManager, jdbc, service, listener);
    }

    @Test
    void indicatorJobWiringBuilds() {
        IndicatorJobConfig config = indicatorConfig(mock(TechnicalIndicatorService.class));

        assertThat(config.dailyIndicatorJob().getName()).isEqualTo("dailyIndicatorJob");
        assertThat(config.indicatorCalculationStep().getName()).isEqualTo("indicatorCalculationStep");
    }

    @Test
    void indicatorTaskletComputesPerSymbolAndSkipsFailures() throws Exception {
        TechnicalIndicatorService service = mock(TechnicalIndicatorService.class);
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class)))
                .thenReturn(List.of("BTC", "ETH", "EMPTY", "BAD"));
        OhlcvData bar = new OhlcvData(BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE);
        when(jdbc.query(anyString(), any(RowMapper.class), eq("BTC"), any(), anyInt())).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), List.of(Map.of(
                        "open", "1", "high", "10", "low", "1", "close", "10", "volume", "1"))));
        when(jdbc.query(anyString(), any(RowMapper.class), eq("ETH"), any(), anyInt())).thenReturn(List.of(bar));
        when(jdbc.query(anyString(), any(RowMapper.class), eq("EMPTY"), any(), anyInt())).thenReturn(List.of());
        when(jdbc.query(anyString(), any(RowMapper.class), eq("BAD"), any(), anyInt())).thenReturn(List.of(bar));
        when(service.computeWithOhlcv(eq("BTC"), any(), any())).thenReturn(
                DailyIndicatorItem.builder().symbol("BTC").tradeDate(LocalDate.of(2025, 1, 1)).obv(5L).build());
        when(service.computeWithOhlcv(eq("ETH"), any(), any())).thenReturn(
                DailyIndicatorItem.builder().symbol("ETH").tradeDate(LocalDate.of(2025, 1, 1)).build());
        when(service.computeWithOhlcv(eq("BAD"), any(), any())).thenThrow(new IllegalStateException("bad series"));
        List<PreparedStatement> statements = new ArrayList<>();
        when(jdbc.batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class)))
                .thenAnswer(inv -> {
                    ParameterizedPreparedStatementSetter<Object> setter = inv.getArgument(3);
                    for (Object item : (Collection<Object>) inv.getArgument(1)) {
                        PreparedStatement ps = mock(PreparedStatement.class);
                        statements.add(ps);
                        setter.setValues(ps, item);
                    }
                    return new int[][] {new int[0]};
                });
        StepContribution contribution = contribution();

        Tasklet tasklet = indicatorConfig(service).indicatorCalculationTasklet("2025-01-01");
        assertThat(tasklet.execute(contribution, null)).isEqualTo(RepeatStatus.FINISHED);

        assertThat(contribution.getWriteCount()).isEqualTo(2);
        assertThat(statements).hasSize(2);
        verify(statements.get(0)).setLong(15, 5L);
        verify(statements.get(1)).setNull(15, java.sql.Types.BIGINT);
    }

    @Test
    void indicatorTaskletFlushesInBatchesOfHundred() throws Exception {
        TechnicalIndicatorService service = mock(TechnicalIndicatorService.class);
        List<String> symbols = new ArrayList<>();
        for (int i = 0; i < 101; i++) {
            symbols.add("S" + i);
        }
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(symbols);
        OhlcvData bar = new OhlcvData(BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE, BigDecimal.TEN, BigDecimal.ONE);
        when(jdbc.query(anyString(), any(RowMapper.class), any(), any(), anyInt())).thenReturn(List.of(bar));
        when(service.computeWithOhlcv(anyString(), any(), any())).thenReturn(
                DailyIndicatorItem.builder().symbol("X").tradeDate(LocalDate.of(2025, 1, 1)).build());

        indicatorConfig(service).indicatorCalculationTasklet("2025-01-01").execute(contribution(), null);

        verify(jdbc, times(2)).batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));
    }

    // ---------- ValidationJobConfig ----------

    private ValidationJobConfig validationConfig() {
        return new ValidationJobConfig(jobRepository, txManager, jdbc, listener, new ObjectMapper());
    }

    @Test
    void validationJobWiringBuilds() {
        ValidationJobConfig config = validationConfig();

        assertThat(config.dataValidationJob().getName()).isEqualTo("dataValidationJob");
        assertThat(config.gapValidationStep().getName()).isEqualTo("gapValidationStep");
        assertThat(config.ohlcvSanityStep().getName()).isEqualTo("ohlcvSanityStep");
        assertThat(config.extremeMovementStep().getName()).isEqualTo("extremeMovementStep");
        assertThat(config.indicatorCoverageStep().getName()).isEqualTo("indicatorCoverageStep");
        assertThat(config.backtestReadinessStep().getName()).isEqualTo("backtestReadinessStep");
    }

    @Test
    void gapAndCoverageTaskletsReportMissingSymbolsOrPass() throws Exception {
        ValidationJobConfig config = validationConfig();
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class)))
                .thenReturn(List.of("ETH"), List.of(), List.of("SOL"), List.of());

        config.gapValidationTasklet("2025-01-02").execute(contribution(), null);
        config.gapValidationTasklet("2025-01-02").execute(contribution(), null);
        config.indicatorCoverageTasklet("2025-01-02").execute(contribution(), null);
        config.indicatorCoverageTasklet("2025-01-02").execute(contribution(), null);

        verify(jdbc).update(anyString(), eq("validation_gap_check"), contains("\"missingCount\":1"));
        verify(jdbc).update(anyString(), eq("validation_indicator_coverage"), contains("\"missingSymbols\":[\"SOL\"]"));
        verify(jdbc, times(4)).update(anyString(), anyString(), anyString());
    }

    @Test
    void sanityAndExtremeMovementTaskletsLogAnomalies() throws Exception {
        ValidationJobConfig config = validationConfig();
        Map<String, Object> anomaly = new HashMap<>(Map.of("symbol", "BTC"));
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(anomaly), List.of(), List.of(anomaly), List.of());

        config.ohlcvSanityTasklet("2025-01-02").execute(contribution(), null);
        config.ohlcvSanityTasklet("2025-01-02").execute(contribution(), null);
        config.extremeMovementTasklet("2025-01-02").execute(contribution(), null);
        config.extremeMovementTasklet("2025-01-02").execute(contribution(), null);

        verify(jdbc).update(anyString(), eq("validation_ohlcv_sanity"), contains("\"anomalyCount\":1"));
        verify(jdbc).update(anyString(), eq("validation_extreme_movement"), contains("\"extremeCount\":1"));
    }

    @Test
    void validationResultSaveFailureDoesNotFailTheStep() throws Exception {
        ValidationJobConfig config = validationConfig();
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of());
        when(jdbc.update(anyString(), anyString(), anyString())).thenThrow(new IllegalStateException("db down"));

        assertThat(config.gapValidationTasklet("2025-01-02").execute(contribution(), null))
                .isEqualTo(RepeatStatus.FINISHED);
    }

    @Test
    void backtestReadinessSeparatesReadyAndAttentionSymbols() throws Exception {
        ValidationJobConfig config = validationConfig();
        Map<String, Object> ready = new HashMap<>(Map.of("symbol", "BTCUSDT", "source", "BINANCE",
                "observation_count", 100L, "stale_days", 0, "internal_gap_days", 0,
                "first_date", LocalDate.of(2024, 1, 1), "last_date", LocalDate.of(2025, 1, 2)));
        Map<String, Object> stale = new HashMap<>(Map.of("symbol", "ETHUSDT", "source", "BINANCE",
                "observation_count", 10L, "stale_days", 3, "internal_gap_days", 1,
                "first_date", LocalDate.of(2024, 12, 1), "last_date", LocalDate.of(2024, 12, 30)));
        when(jdbc.queryForList(anyString(), any(Object[].class))).thenReturn(List.of(ready, stale), List.of(ready));

        config.backtestReadinessTasklet("2025-01-02", " btcusdt, ETHUSDT ,btcusdt,").execute(contribution(), null);
        config.backtestReadinessTasklet("2025-01-02", "BTCUSDT").execute(contribution(), null);

        verify(jdbc).update(anyString(), eq("validation_backtest_readiness"), contains("\"attentionCount\":1"));
        verify(jdbc).update(anyString(), eq("validation_backtest_readiness"), contains("\"attentionCount\":0"));
    }

    @Test
    void backtestReadinessRejectsEmptySymbolList() {
        ValidationJobConfig config = validationConfig();

        assertThatThrownBy(() -> config.backtestReadinessTasklet("2025-01-02", " , ").execute(contribution(), null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("must not be empty");
        verify(jdbc, never()).queryForList(anyString(), any(Object[].class));
    }
}
