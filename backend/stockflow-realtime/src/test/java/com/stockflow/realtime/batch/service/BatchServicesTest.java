package com.stockflow.realtime.batch.service;

import com.stockflow.realtime.batch.service.BinanceDailyOhlcvBackfillService.BackfillResult;
import com.stockflow.realtime.service.RedisPriceService;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class BatchServicesTest {

    // ---------- PrevCloseSyncService ----------

    @Test
    void prevCloseSyncLoadsLatestClosesIntoRedis() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        doAnswer(inv -> {
            RowCallbackHandler handler = inv.getArgument(1);
            for (String[] row : new String[][] {{"BTC", "100.5"}, {"ETH", "50"}}) {
                ResultSet rs = mock(ResultSet.class);
                when(rs.getString("symbol")).thenReturn(row[0]);
                when(rs.getBigDecimal("close")).thenReturn(new BigDecimal(row[1]));
                handler.processRow(rs);
            }
            return null;
        }).when(jdbc).query(anyString(), any(RowCallbackHandler.class));
        when(redis.loadPreviousCloses(anyMap())).thenReturn(2);

        int loaded = new PrevCloseSyncService(jdbc, redis).syncFromDailyOhlcv();

        assertThat(loaded).isEqualTo(2);
        ArgumentCaptor<Map<String, BigDecimal>> captor = ArgumentCaptor.forClass(Map.class);
        verify(redis).loadPreviousCloses(captor.capture());
        assertThat(captor.getValue()).containsEntry("BTC", new BigDecimal("100.5")).hasSize(2);
    }

    @Test
    void prevCloseSyncSkipsWhenNoDailyData() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        RedisPriceService redis = mock(RedisPriceService.class);

        assertThat(new PrevCloseSyncService(jdbc, redis).syncFromDailyOhlcv()).isZero();
        verify(redis, never()).loadPreviousCloses(anyMap());
    }

    // ---------- BinanceDailyOhlcvBackfillService ----------

    private static final LocalDate FROM = LocalDate.of(2025, 1, 1);

    /** 인터셉터로 Binance 응답을 대체한다(서비스가 requestFactory 를 덮어써도 인터셉터는 유지된다). */
    private static RestClient.Builder binanceReturning(List<String> requested, java.util.function.Function<String, String> bodyFor,
                                                      HttpStatus status) {
        return RestClient.builder().requestInterceptor((request, body, execution) -> {
            requested.add(request.getURI().toString());
            MockClientHttpResponse response = new MockClientHttpResponse(
                    bodyFor.apply(request.getURI().getQuery()).getBytes(StandardCharsets.UTF_8), status);
            response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
            return response;
        });
    }

    private static final String ONE_KLINE =
            "[[1735689600000,\"100\",\"110\",\"90\",\"105\",\"12.5\",1735775999999,\"0\",1,\"0\",\"0\",\"0\"]]";

    @Test
    void backfillWritesBarsForRequestedSymbols() throws Exception {
        List<String> requested = new ArrayList<>();
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        List<PreparedStatement> statements = new ArrayList<>();
        when(jdbc.batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class)))
                .thenAnswer(inv -> {
                    Collection<Object> items = inv.getArgument(1);
                    ParameterizedPreparedStatementSetter<Object> setter = inv.getArgument(3);
                    for (Object item : items) {
                        PreparedStatement ps = mock(PreparedStatement.class);
                        statements.add(ps);
                        setter.setValues(ps, item);
                    }
                    return new int[][] {new int[items.size()]};
                });
        BinanceDailyOhlcvBackfillService service =
                new BinanceDailyOhlcvBackfillService(jdbc, binanceReturning(requested, q -> ONE_KLINE, HttpStatus.OK));

        BackfillResult result = service.backfill(FROM, FROM, List.of(" btcusdt ", "BTCUSDT", "", "ethusdt"));

        assertThat(result.requestedSymbolCount()).isEqualTo(2);
        assertThat(result.succeededSymbolCount()).isEqualTo(2);
        assertThat(result.writtenBarCount()).isEqualTo(2);
        assertThat(result.failures()).isEmpty();
        assertThat(requested).hasSize(2).allMatch(uri -> uri.contains("/api/v3/klines") && uri.contains("interval=1d"));
        assertThat(statements).hasSize(2);
        verify(statements.get(0)).setBigDecimal(5, new BigDecimal("100"));
        verify(statements.get(0)).setObject(2, LocalDate.of(2025, 1, 1));
    }

    @Test
    void backfillSplitsLongRangesIntoThousandDayWindows() {
        List<String> requested = new ArrayList<>();
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        BinanceDailyOhlcvBackfillService service =
                new BinanceDailyOhlcvBackfillService(jdbc, binanceReturning(requested, q -> "[]", HttpStatus.OK));

        BackfillResult result = service.backfill(FROM, FROM.plusDays(1500), List.of("BTCUSDT"));

        assertThat(requested).hasSize(2);
        assertThat(result.writtenBarCount()).isZero(); // 빈 응답은 쓰기 생략
        verify(jdbc, never()).batchUpdate(anyString(), any(Collection.class), anyInt(), any(ParameterizedPreparedStatementSetter.class));
    }

    @Test
    void backfillReportsPerSymbolFailures() {
        List<String> requested = new ArrayList<>();
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        AtomicInteger calls = new AtomicInteger();
        BinanceDailyOhlcvBackfillService errorService = new BinanceDailyOhlcvBackfillService(jdbc,
                binanceReturning(requested, q -> "{\"code\":-1121}", HttpStatus.BAD_REQUEST));
        BackfillResult errored = errorService.backfill(FROM, FROM, List.of("NOPEUSDT"));
        assertThat(errored.succeededSymbolCount()).isZero();
        assertThat(errored.failures()).singleElement().asString().contains("NOPEUSDT").contains("400");

        BinanceDailyOhlcvBackfillService invalidService = new BinanceDailyOhlcvBackfillService(jdbc,
                binanceReturning(requested, q -> calls.incrementAndGet() > 0 ? "{\"not\":\"an array\"}" : "", HttpStatus.OK));
        BackfillResult invalid = invalidService.backfill(FROM, FROM, List.of("BTCUSDT"));
        assertThat(invalid.failures()).singleElement().asString().contains("invalid kline response");
    }

    @Test
    void backfillUsesKnownSymbolsWhenNoneRequestedAndValidatesRange() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of());
        BinanceDailyOhlcvBackfillService service =
                new BinanceDailyOhlcvBackfillService(jdbc, RestClient.builder());

        BackfillResult result = service.backfill(FROM, FROM, null);
        assertThat(result.requestedSymbolCount()).isZero();

        assertThatThrownBy(() -> service.backfill(FROM.plusDays(1), FROM, List.of("BTC")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.backfill(null, FROM, List.of("BTC")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
