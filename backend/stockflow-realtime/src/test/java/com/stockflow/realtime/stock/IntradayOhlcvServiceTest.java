package com.stockflow.realtime.stock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** 출처(source) 필터가 ohlcv_1m·틱 보충·cutoff 서브쿼리에 모두 바인드 파라미터로 들어가는지 검증한다. */
class IntradayOhlcvServiceTest {

    private static final Instant TO = Instant.parse("2026-10-06T12:00:00Z");
    private static final Instant FROM = TO.minusSeconds(3600);

    private JdbcTemplate jdbc;
    private SymbolSourceResolver resolver;
    private IntradayOhlcvService service;
    private String sql;
    private Object[] args;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(invocation -> {
            Object[] raw = invocation.getArguments();
            sql = (String) raw[0];
            // Mockito 버전에 따라 가변 인자가 펼쳐져 오기도, 배열 하나로 오기도 한다.
            args = raw.length == 3 && raw[2] instanceof Object[] array
                    ? array : Arrays.copyOfRange(raw, 2, raw.length);
            return List.of();
        });
        resolver = mock(SymbolSourceResolver.class);
        service = new IntradayOhlcvService(jdbc, resolver);
    }

    @Test
    void sourceGiven_filtersAllThreeSubqueriesWithBindArgs() {
        service.getIntraday("aapl", "1m", FROM, TO, "alpaca");

        String sql = capturedSql();
        Object[] args = capturedArgs();
        assertThat(sql).doesNotContain("@SRC@");
        assertThat(countOf(sql, "symbol = ? AND source = ?")).isEqualTo(3);
        assertThat(sql).contains("t.symbol = ? AND source = ?");
        assertThat(placeholders(sql)).isEqualTo(args.length);
        assertThat(Arrays.asList(args)).filteredOn("ALPACA"::equals).hasSize(3);
        assertThat(args).doesNotContain("alpaca");
    }

    @Test
    void sourceGiven_argsKeepOrderOfPlaceholders() {
        service.getIntraday("aapl", "5m", FROM, TO, " alpaca ");

        Object[] args = capturedArgs();
        // last_candle(cutoff 기본값, symbol, source, from, to) → ohlcv_1m → market_ticks → 버킷
        assertThat(args[1]).isEqualTo("AAPL");
        assertThat(args[2]).isEqualTo("ALPACA");
        assertThat(args[5]).isEqualTo("AAPL");
        assertThat(args[6]).isEqualTo("ALPACA");
        assertThat(args[9]).isEqualTo("AAPL");
        assertThat(args[10]).isEqualTo("ALPACA");
        // 틱 보충 하한은 end-30분(from 보다 뒤), 나머지는 from/to
        Timestamp start = Timestamp.from(FROM);
        Timestamp end = Timestamp.from(TO);
        assertThat(args[0]).isEqualTo(start);
        assertThat(args[3]).isEqualTo(start);
        assertThat(args[4]).isEqualTo(end);
        assertThat(args[7]).isEqualTo(start);
        assertThat(args[8]).isEqualTo(end);
        assertThat(args[11]).isEqualTo(Timestamp.from(TO.minusSeconds(1800)));
        assertThat(args[12]).isEqualTo(end);
        assertThat(args[args.length - 2]).isEqualTo(300);
        assertThat(args[args.length - 1]).isEqualTo(300);
    }

    @Test
    void sourceNull_keepsLegacySqlAndArgs() {
        service.getIntraday("aapl", "1m", FROM, TO);

        String sql = capturedSql();
        Object[] args = capturedArgs();
        assertThat(sql).doesNotContain("source").doesNotContain("@SRC@");
        assertThat(placeholders(sql)).isEqualTo(args.length);
        assertThat(args).hasSize(12);
        assertThat(args[1]).isEqualTo("AAPL");
        assertThat(args[4]).isEqualTo("AAPL");
        assertThat(args[7]).isEqualTo("AAPL");
    }

    @Test
    void blankSource_behavesLikeNull() {
        service.getIntraday("aapl", "1m", FROM, TO, "  ");

        assertThat(capturedSql()).doesNotContain("source");
        assertThat(capturedArgs()).hasSize(12);
    }

    @Test
    void sourceNull_asksResolverAndFiltersByItsAnswer() {
        when(resolver.latestSource("aapl")).thenReturn(Optional.of("ALPACA"));

        service.getIntraday("aapl", "1m", FROM, TO, null);

        assertThat(countOf(capturedSql(), "symbol = ? AND source = ?")).isEqualTo(3);
        assertThat(Arrays.asList(capturedArgs())).filteredOn("ALPACA"::equals).hasSize(3);
    }

    @Test
    void explicitSource_winsAndResolverIsNotConsulted() {
        when(resolver.latestSource("aapl")).thenReturn(Optional.of("ALPACA"));

        service.getIntraday("aapl", "1m", FROM, TO, "simulator");

        verifyNoInteractions(resolver);
        assertThat(Arrays.asList(capturedArgs())).filteredOn("SIMULATOR"::equals).hasSize(3);
    }

    @Test
    void resolverEmpty_keepsLegacySqlAndArgs() {
        when(resolver.latestSource("aapl")).thenReturn(Optional.empty());

        service.getIntraday("aapl", "1m", FROM, TO, null);

        verify(resolver).latestSource("aapl");
        assertThat(capturedSql()).doesNotContain("source");
        assertThat(capturedArgs()).hasSize(12);
    }

    @Test
    void fourArgOverload_neverConsultsResolver() {
        service.getIntraday("btcusdt", "1m", FROM, TO);

        verify(resolver, never()).latestSource(any());
    }

    private String capturedSql() {
        assertThat(sql).isNotNull();
        return sql;
    }

    private Object[] capturedArgs() {
        assertThat(args).isNotNull();
        return args;
    }

    /** SQL 주석(-- ...)에 들어 있는 물음표는 바인드 자리가 아니다. */
    private static int placeholders(String sql) {
        return countOf(sql.replaceAll("(?m)--.*$", ""), "?");
    }

    private static int countOf(String text, String token) {
        int count = 0;
        for (int i = text.indexOf(token); i >= 0; i = text.indexOf(token, i + token.length())) {
            count++;
        }
        return count;
    }
}
