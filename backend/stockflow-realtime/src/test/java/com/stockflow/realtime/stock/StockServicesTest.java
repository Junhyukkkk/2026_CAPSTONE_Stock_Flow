package com.stockflow.realtime.stock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.service.RedisPriceService;
import com.stockflow.realtime.stock.InstrumentRepository.InstrumentRow;
import com.stockflow.realtime.stock.dto.IndicatorResponse;
import com.stockflow.realtime.stock.dto.InstrumentCreateRequest;
import com.stockflow.realtime.stock.dto.InstrumentResponse;
import com.stockflow.realtime.stock.dto.InstrumentUpdateRequest;
import com.stockflow.realtime.stock.dto.IntradayOhlcvResponse;
import com.stockflow.realtime.stock.dto.OhlcvResponse;
import com.stockflow.realtime.testsupport.JdbcTestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class StockServicesTest {

    private static final Instant NOW = Instant.parse("2025-01-02T00:00:00Z");

    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        jdbc = mock(JdbcTemplate.class);
    }

    private void stubRows(List<Map<String, Object>> rows) {
        when(jdbc.query(anyString(), any(RowMapper.class), any(Object[].class))).thenAnswer(inv ->
                JdbcTestSupport.mapRows(inv.getArgument(1, RowMapper.class), rows));
    }

    private static <T> T json(Class<T> type, String body) {
        try {
            return new ObjectMapper().readValue(body, type);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // ---------- InstrumentRepository ----------

    private static Map<String, Object> instrumentRow() {
        return Map.of("symbol", "BTCUSDT", "name", "Bitcoin", "market_type", "CRYPTO", "exchange", "BINANCE",
                "is_active", true, "last_seen_at", NOW);
    }

    @Test
    void instrumentRepositoryBuildsFilteredQueries() {
        InstrumentRepository repository = new InstrumentRepository(jdbc);
        stubRows(List.of(instrumentRow()));

        List<InstrumentRow> rows = repository.findAll("crypto", true);

        assertThat(rows).singleElement().satisfies(r -> {
            assertThat(r.symbol()).isEqualTo("BTCUSDT");
            assertThat(r.active()).isTrue();
            assertThat(r.lastSeenAt()).isEqualTo(NOW);
        });
        verify(jdbc).query(contains("market_type = ? AND is_active = true"), any(RowMapper.class), eq("CRYPTO"));

        repository.findAll(null, false);
        verify(jdbc).query(eq("SELECT symbol, name, market_type, exchange, is_active, last_seen_at FROM instruments WHERE 1=1 ORDER BY last_seen_at DESC"),
                any(RowMapper.class), any(Object[].class));
    }

    @Test
    void instrumentRepositoryFindBySymbolAndWrites() {
        InstrumentRepository repository = new InstrumentRepository(jdbc);
        stubRows(List.of());
        assertThat(repository.findBySymbol("btc")).isEmpty();
        stubRows(List.of(instrumentRow()));
        assertThat(repository.findBySymbol("btc")).isPresent();

        repository.upsert("btc", "crypto", "binance", null);
        verify(jdbc).update(eq("SELECT register_instrument(?, ?, ?, ?)"), eq("BTC"), eq("CRYPTO"), eq("BINANCE"), eq("BTC"));
        repository.upsert("btc", "crypto", "binance", "Bitcoin");
        verify(jdbc).update(anyString(), eq("BTC"), eq("CRYPTO"), eq("BINANCE"), eq("Bitcoin"));

        when(jdbc.update(anyString(), any(Object[].class))).thenReturn(1);
        assertThat(repository.update("btc", " Bitcoin ", " binance ")).isEqualTo(1);
        verify(jdbc).update(eq("UPDATE instruments SET name = ?, exchange = ? WHERE symbol = ?"),
                eq("Bitcoin"), eq("BINANCE"), eq("BTC"));
        assertThat(repository.update("btc", " ", null)).isZero();
        assertThat(repository.update("btc", null, "x")).isEqualTo(1);

        assertThat(repository.setActive("btc", false)).isEqualTo(1);
        verify(jdbc).update(contains("is_active = ?"), eq(false), eq("BTC"));
    }

    // ---------- InstrumentService ----------

    private InstrumentService instrumentService(InstrumentRepository repository, RedisPriceService redis) {
        return new InstrumentService(repository, redis);
    }

    private static InstrumentRow row() {
        return new InstrumentRow("BTCUSDT", "Bitcoin", "CRYPTO", "BINANCE", true, NOW);
    }

    @Test
    void instrumentServiceEnrichesWithLatestPrice() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        when(repository.findAll("CRYPTO", true)).thenReturn(List.of(row()));
        when(repository.findBySymbol("BTCUSDT")).thenReturn(Optional.of(row()));
        when(repository.findBySymbol("ETHUSDT")).thenReturn(Optional.empty());
        when(redis.getLatestPrice("BTCUSDT")).thenReturn(PriceSnapshot.builder().symbol("BTCUSDT")
                .price(BigDecimal.TEN).change(BigDecimal.ONE).changePercent(BigDecimal.valueOf(10)).build());
        InstrumentService service = instrumentService(repository, redis);

        List<InstrumentResponse> list = service.list("CRYPTO", true);
        assertThat(list.get(0).getCurrentPrice()).isEqualByComparingTo("10");
        assertThat(list.get(0).getChange()).isEqualByComparingTo("1");

        assertThat(service.getBySymbol("btcusdt")).isPresent();
        assertThat(service.getBySymbol("ethusdt")).isEmpty();
    }

    @Test
    void instrumentServiceLeavesPriceEmptyWhenNotCached() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        when(repository.findBySymbol("BTCUSDT")).thenReturn(Optional.of(row()));

        InstrumentResponse response = instrumentService(repository, redis).getBySymbol("BTCUSDT").orElseThrow();

        assertThat(response.getCurrentPrice()).isNull();
        assertThat(response.getChangePercent()).isNull();
    }

    @Test
    void instrumentServiceCreateUpdateDeactivate() {
        InstrumentRepository repository = mock(InstrumentRepository.class);
        RedisPriceService redis = mock(RedisPriceService.class);
        when(repository.findBySymbol("BTCUSDT")).thenReturn(Optional.of(row()));
        when(repository.findBySymbol("NOPE")).thenReturn(Optional.empty());
        when(repository.setActive("BTCUSDT", false)).thenReturn(1);
        when(repository.setActive("NOPE", false)).thenReturn(0);
        InstrumentService service = instrumentService(repository, redis);

        InstrumentCreateRequest create = json(InstrumentCreateRequest.class,
                "{\"symbol\":\"BTCUSDT\",\"marketType\":\"CRYPTO\",\"exchange\":\"BINANCE\",\"name\":\"Bitcoin\"}");
        assertThat(service.create(create).getSymbol()).isEqualTo("BTCUSDT");
        verify(repository).upsert("BTCUSDT", "CRYPTO", "BINANCE", "Bitcoin");

        assertThat(service.update("NOPE", json(InstrumentUpdateRequest.class, "{\"name\":\"x\"}"))).isEmpty();
        assertThat(service.update("BTCUSDT", json(InstrumentUpdateRequest.class, "{\"name\":\"x\"}"))).isPresent();
        verify(repository).update("BTCUSDT", "x", null);
        service.update("BTCUSDT", json(InstrumentUpdateRequest.class, "{}"));
        verify(repository, never()).update(eq("BTCUSDT"), eq(null), eq(null));

        assertThat(service.deactivate("btcusdt")).isTrue();
        assertThat(service.deactivate("nope")).isFalse();
        verify(repository, never()).setActive(eq("btcusdt"), anyBoolean());
    }

    // ---------- IndicatorHistoryService ----------

    private static Map<String, Object> indicatorRow(boolean withObv) {
        Map<String, Object> row = new HashMap<>();
        row.put("symbol", "BTCUSDT");
        row.put("trade_date", LocalDate.of(2025, 1, 1));
        for (String column : List.of("ma5", "ma20", "ma60", "rsi14", "macd", "macd_signal", "macd_hist",
                "bb_upper", "bb_lower", "stoch_k", "stoch_d", "atr14")) {
            row.put(column, "1.5");
        }
        if (withObv) {
            row.put("obv", 123L);
        }
        return row;
    }

    @Test
    void indicatorHistoryMapsRowsForEveryQueryShape() {
        IndicatorHistoryService service = new IndicatorHistoryService(jdbc);
        stubRows(List.of(indicatorRow(true)));

        List<IndicatorResponse> range = service.getIndicators("btc", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2));
        assertThat(range).singleElement().satisfies(r -> {
            assertThat(r.getObv()).isEqualTo(123L);
            assertThat(r.getBbMiddle()).isEqualByComparingTo("1.5"); // bbMiddle == ma20
            assertThat(r.getTradeDate()).isEqualTo(LocalDate.of(2025, 1, 1));
        });
        assertThat(service.getLatest("btc")).isPresent();
        assertThat(service.getHistory("btc", 30)).hasSize(1);
        verify(jdbc).query(contains("LIMIT ?"), any(RowMapper.class), eq("BTC"), eq(30));

        stubRows(List.of(indicatorRow(false)));
        assertThat(service.getLatest("btc").orElseThrow().getObv()).isNull();
        stubRows(List.of());
        assertThat(service.getLatest("btc")).isEmpty();
    }

    // ---------- OhlcvHistoryService ----------

    @Test
    void ohlcvHistoryMapsDailyBars() {
        stubRows(List.of(Map.of("symbol", "BTCUSDT", "trade_date", LocalDate.of(2025, 1, 1), "open", "1", "high", "2",
                "low", "0.5", "close", "1.5", "volume", "100", "tick_count", 42L)));

        List<OhlcvResponse> bars = new OhlcvHistoryService(jdbc)
                .getOhlcv("btcusdt", LocalDate.of(2025, 1, 1), LocalDate.of(2025, 1, 2));

        assertThat(bars).singleElement().satisfies(b -> {
            assertThat(b.getTickCount()).isEqualTo(42L);
            assertThat(b.getClose()).isEqualByComparingTo("1.5");
        });
    }

    // ---------- IntradayOhlcvService ----------

    @Test
    void intradayRejectsBadIntervalAndRanges() {
        IntradayOhlcvService service = new IntradayOhlcvService(jdbc);
        Instant from = Instant.parse("2025-01-01T00:00:00Z");

        assertThatThrownBy(() -> service.getIntraday("btc", "2m", from, from.plusSeconds(60)))
                .isInstanceOfSatisfying(ResponseStatusException.class, e -> {
                    assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    assertThat(e.getReason()).contains("interval");
                });
        assertThatThrownBy(() -> service.getIntraday("btc", "1m", from, from))
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("from");
        assertThatThrownBy(() -> service.getIntraday("btc", "1m", from, from.plusSeconds(8 * 86400L)))
                .isInstanceOf(ResponseStatusException.class).hasMessageContaining("최대 7일");
    }

    @Test
    void intradayQueriesWithBucketSecondsAndMapsRows() {
        IntradayOhlcvService service = new IntradayOhlcvService(jdbc);
        Instant from = Instant.parse("2025-01-01T00:00:00Z");
        Map<String, Object> row = new HashMap<>();
        row.put("bucket", from);
        row.put("open", "1");
        row.put("high", "2");
        row.put("low", "0.5");
        row.put("close", "1.5");
        row.put("volume", "10");
        row.put("tick_count", 7L);
        Map<String, Object> noTicks = new HashMap<>(row);
        noTicks.put("tick_count", null);
        stubRows(List.of(row, noTicks));

        List<IntradayOhlcvResponse> candles = service.getIntraday("btcusdt", "5m", from, from.plusSeconds(3600));

        assertThat(candles).hasSize(2);
        assertThat(candles.get(0).getSymbol()).isEqualTo("BTCUSDT");
        assertThat(candles.get(0).getTickCount()).isEqualTo(7L);
        assertThat(candles.get(1).getTickCount()).isNull();
        verify(jdbc).query(contains("ohlcv_1m"), any(RowMapper.class), any(Object[].class));
    }

    @Test
    void intradayDefaultsToLastDayAndClampsTopUpWindow() {
        IntradayOhlcvService service = new IntradayOhlcvService(jdbc);
        stubRows(List.of());

        assertThat(service.getIntraday("btc", "1h", null, null)).isEmpty();
        // 구간이 보충 윈도우(30분)보다 짧으면 from 을 그대로 하한으로 사용
        Instant from = Instant.parse("2025-01-01T00:00:00Z");
        assertThat(service.getIntraday("btc", "1m", from, from.plusSeconds(300))).isEmpty();
    }
}
