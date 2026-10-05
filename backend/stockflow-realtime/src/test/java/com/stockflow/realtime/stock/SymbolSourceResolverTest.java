package com.stockflow.realtime.stock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SymbolSourceResolverTest {

    private JdbcTemplate jdbc;
    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-06T12:00:00Z"));
    private SymbolSourceResolver resolver;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
        Clock clock = new Clock() {
            @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now.get(); }
        };
        resolver = new SymbolSourceResolver(jdbc, clock);
    }

    @Test
    void sqlIsTimeBoundedAndSymbolIsBoundUpperCased() {
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of("ALPACA"));

        assertThat(resolver.latestSource(" aapl ")).contains("ALPACA");

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbc).queryForList(sql.capture(), eq(String.class), eq("AAPL"));
        assertThat(sql.getValue())
                .contains("bucket >= now() - interval '3 days'")
                .contains("symbol = ?")
                .contains("ORDER BY max(bucket) DESC, source ASC")
                .contains("LIMIT 1")
                .doesNotContain("AAPL");
    }

    @Test
    void noRecentBarsGivesEmptyAndIsNotCached() {
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class))).thenReturn(List.of());

        assertThat(resolver.latestSource("zzz")).isEmpty();
        assertThat(resolver.latestSource("zzz")).isEmpty();

        verify(jdbc, times(2)).queryForList(anyString(), eq(String.class), eq("ZZZ"));
    }

    @Test
    void blankSymbolIsEmptyWithoutQuery() {
        assertThat(resolver.latestSource(" ")).isEmpty();
        assertThat(resolver.latestSource(null)).isEmpty();

        org.mockito.Mockito.verifyNoInteractions(jdbc);
    }

    @Test
    void resultIsCachedFor30SecondsThenRefreshed() {
        when(jdbc.queryForList(anyString(), eq(String.class), any(Object[].class)))
                .thenReturn(List.of("ALPACA"), List.of("SIMULATOR"));

        assertThat(resolver.latestSource("AAPL")).contains("ALPACA");
        now.set(now.get().plusSeconds(29));
        assertThat(resolver.latestSource("aapl")).contains("ALPACA");
        verify(jdbc, times(1)).queryForList(anyString(), eq(String.class), eq("AAPL"));

        now.set(now.get().plusSeconds(2));
        assertThat(resolver.latestSource("AAPL")).contains("SIMULATOR");
        verify(jdbc, times(2)).queryForList(anyString(), eq(String.class), eq("AAPL"));
    }
}
