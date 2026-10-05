package com.stockflow.realtime.stock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** 출처(source) 필터가 ohlcv_1m·틱 보충·cutoff 서브쿼리에 모두 바인드 파라미터로 들어가는지 검증한다. */
class IntradayOhlcvServiceTest {

    private static final Instant TO = Instant.parse("2026-10-06T12:00:00Z");
    private static final Instant FROM = TO.minusSeconds(3600);

    private JdbcTemplate jdbc;
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
        service = new IntradayOhlcvService(jdbc);
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
