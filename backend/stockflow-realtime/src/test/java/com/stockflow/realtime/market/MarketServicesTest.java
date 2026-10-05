package com.stockflow.realtime.market;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.market.dto.MarketOverviewResponse;
import com.stockflow.realtime.service.RedisPriceService;
import com.stockflow.realtime.stock.InstrumentRepository;
import com.stockflow.realtime.stock.InstrumentRepository.InstrumentRow;
import com.stockflow.realtime.storage.MarketTickPreviewService;
import com.stockflow.realtime.storage.dto.StorageOverviewResponse;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class MarketServicesTest {

    private static InstrumentRow instrument(String symbol) {
        return new InstrumentRow(symbol, symbol + "-name", "CRYPTO", "BINANCE", true, Instant.EPOCH);
    }

    private static PriceSnapshot snapshot(String symbol, String price, String changePercent) {
        return PriceSnapshot.builder().symbol(symbol).price(new BigDecimal(price))
                .change(BigDecimal.ONE).changePercent(new BigDecimal(changePercent)).build();
    }

    // ---------- MarketOverviewService ----------

    @Test
    void overviewIsEmptyWithoutActiveInstruments() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        when(repository.findAll("CRYPTO", true)).thenReturn(List.of());

        MarketOverviewResponse response = new MarketOverviewService(repository, mock(RedisPriceService.class),
                mock(JdbcTemplate.class)).getOverview("CRYPTO");

        assertThat(response.getTotalActive()).isZero();
        assertThat(response.getTopGainers()).isEmpty();
        assertThat(response.getTopLosers()).isEmpty();
    }

    @Test
    void overviewRanksGainersAndLosersAndFallsBackToPrevClose() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(repository.findAll(null, true)).thenReturn(List.of(
                instrument("UP"), instrument("DOWN"), instrument("STALE"), instrument("GHOST")));
        when(redis.getLatestPrice("UP")).thenReturn(snapshot("UP", "110", "10.00"));
        when(redis.getLatestPrice("DOWN")).thenReturn(snapshot("DOWN", "90", "-10.00"));
        // STALE: 실시간 시세 없음, 전일 종가만 있음 → 0% 로 포함. GHOST: 둘 다 없음 → 제외.
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), List.of(
                        Map.<String, Object>of("symbol", "UP", "close", "100"),
                        Map.<String, Object>of("symbol", "DOWN", "close", "100"),
                        Map.<String, Object>of("symbol", "STALE", "close", "50"))));

        MarketOverviewResponse response = new MarketOverviewService(repository, redis, jdbc).getOverview(null);

        assertThat(response.getTotalActive()).isEqualTo(4);
        assertThat(response.getWithRealtimePrice()).isEqualTo(2);
        assertThat(response.getTopGainers()).extracting("symbol").containsExactly("UP", "STALE", "DOWN");
        assertThat(response.getTopLosers()).extracting("symbol").containsExactly("DOWN", "STALE", "UP");
        assertThat(response.getTopGainers().get(1).getCurrentPrice()).isEqualByComparingTo("50");
        assertThat(response.getTopGainers().get(1).getChangePercent()).isEqualByComparingTo("0");
    }

    @Test
    void overviewLimitsRankingsToTen() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        List<InstrumentRow> rows = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            rows.add(instrument("S" + i));
            when(redis.getLatestPrice("S" + i)).thenReturn(snapshot("S" + i, "10", String.valueOf(i)));
        }
        when(repository.findAll("CRYPTO", true)).thenReturn(rows);
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());

        MarketOverviewResponse response = new MarketOverviewService(repository, redis, jdbc).getOverview("CRYPTO");

        assertThat(response.getTopGainers()).hasSize(10);
        assertThat(response.getTopGainers().get(0).getSymbol()).isEqualTo("S14");
        assertThat(response.getTopLosers().get(0).getSymbol()).isEqualTo("S0");
    }

    // ---------- MarketTickPreviewService ----------

    @Test
    void tickPreviewCombinesCountTicksAndOneMinuteBars() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class))).thenReturn(12345L);
        when(jdbc.queryForObject(anyString(), eq(Boolean.class))).thenReturn(true);
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv -> {
            RowMapper mapper = inv.getArgument(1, RowMapper.class);
            String sql = inv.getArgument(0, String.class);
            Map<String, Object> row = sql.contains("market_ticks_1m")
                    ? Map.of("bucket", Instant.EPOCH, "symbol", "BTC", "source", "BINANCE", "open", "1", "high", "2",
                            "low", "0.5", "close", "1.5", "volume", "9")
                    : Map.of("id", 1L, "source", "BINANCE", "symbol", "BTC", "trade_id", "t1", "price", "10",
                            "volume", "1", "ts", Instant.EPOCH, "ingested_at", Instant.EPOCH);
            return JdbcTestSupport.mapRows(mapper, List.of(row));
        });

        StorageOverviewResponse response = new MarketTickPreviewService(jdbc).overview(500);

        assertThat(response.approximateTickRows()).isEqualTo(12345L);
        assertThat(response.oneMinuteAggregateAvailable()).isTrue();
        assertThat(response.recentTicks()).singleElement().satisfies(t -> {
            assertThat(t.tradeId()).isEqualTo("t1");
            assertThat(t.ingestedAt()).isEqualTo(Instant.EPOCH);
        });
        assertThat(response.recentOneMinuteBars()).singleElement()
                .satisfies(b -> assertThat(b.close()).isEqualByComparingTo("1.5"));
        org.mockito.Mockito.verify(jdbc).query(org.mockito.ArgumentMatchers.contains("market_ticks\n"), any(RowMapper.class), eq(100));
        org.mockito.Mockito.verify(jdbc).query(org.mockito.ArgumentMatchers.contains("market_ticks_1m"), any(RowMapper.class), eq(30));
    }

    @Test
    void tickPreviewSkipsBarsWhenAggregateMissingAndDefaultsLimit() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class))).thenReturn(null);
        when(jdbc.queryForObject(anyString(), eq(Boolean.class))).thenReturn(false);
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());

        StorageOverviewResponse response = new MarketTickPreviewService(jdbc).overview(null);

        assertThat(response.approximateTickRows()).isZero();
        assertThat(response.oneMinuteAggregateAvailable()).isFalse();
        assertThat(response.recentOneMinuteBars()).isEmpty();
        org.mockito.Mockito.verify(jdbc).query(anyString(), any(RowMapper.class), eq(25));
    }

    @Test
    void tickPreviewReturnsEmptyOverviewOnDatabaseError() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject(anyString(), eq(Long.class))).thenThrow(new DataAccessResourceFailureException("db"));

        StorageOverviewResponse response = new MarketTickPreviewService(jdbc).overview(0);

        assertThat(response.approximateTickRows()).isZero();
        assertThat(response.recentTicks()).isEmpty();
    }
}
