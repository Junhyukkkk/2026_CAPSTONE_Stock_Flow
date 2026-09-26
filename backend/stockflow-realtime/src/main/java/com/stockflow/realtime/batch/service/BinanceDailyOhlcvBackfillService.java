package com.stockflow.realtime.batch.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Binance REST 일봉을 symbol_daily_ohlcv에 UPSERT한다.
 * 실시간 틱이 수집되지 않았던 날짜도 복원할 수 있도록 별도 경로로 둔다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BinanceDailyOhlcvBackfillService {

    private static final String SOURCE = "BINANCE";
    private static final String MARKET_TYPE = "CRYPTO";
    private static final int MAX_DAYS_PER_REQUEST = 1_000;
    private static final long REQUEST_INTERVAL_MS = 75L;
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(15);

    private final JdbcTemplate jdbcTemplate;
    private final RestClient.Builder restClientBuilder;

    public BackfillResult backfill(LocalDate from, LocalDate to, List<String> requestedSymbols) {
        if (from == null || to == null || from.isAfter(to)) {
            throw new IllegalArgumentException("from must be on or before to");
        }

        List<String> symbols = requestedSymbols == null || requestedSymbols.isEmpty()
                ? findKnownCryptoSymbols()
                : normalizeSymbols(requestedSymbols);
        if (symbols.isEmpty()) {
            return new BackfillResult(from, to, 0, 0, 0, List.of());
        }

        RestClient client = binanceClient();
        int succeeded = 0;
        int writtenBars = 0;
        List<String> failures = new ArrayList<>();

        for (String symbol : symbols) {
            try {
                List<DailyBar> bars = fetchDailyBars(client, symbol, from, to);
                writeBars(bars);
                succeeded++;
                writtenBars += bars.size();
            } catch (RestClientException | IllegalArgumentException ex) {
                log.warn("Binance daily backfill skipped: symbol={} reason={}", symbol, ex.getMessage());
                if (failures.size() < 20) {
                    failures.add(symbol + ": " + conciseMessage(ex));
                }
            }
            pauseBetweenRequests();
        }

        return new BackfillResult(from, to, symbols.size(), succeeded, writtenBars, List.copyOf(failures));
    }

    private List<String> findKnownCryptoSymbols() {
        return jdbcTemplate.queryForList(
                """
                SELECT symbol
                FROM symbol_daily_ohlcv
                WHERE source = ? AND market_type = ?
                GROUP BY symbol
                ORDER BY symbol
                """,
                String.class, SOURCE, MARKET_TYPE);
    }

    private RestClient binanceClient() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(CONNECT_TIMEOUT);
        requestFactory.setReadTimeout(READ_TIMEOUT);
        return restClientBuilder
                .baseUrl("https://api.binance.com")
                .requestFactory(requestFactory)
                .build();
    }

    private List<String> normalizeSymbols(List<String> requestedSymbols) {
        Set<String> normalized = new LinkedHashSet<>();
        for (String raw : requestedSymbols) {
            if (raw != null && !raw.isBlank()) {
                normalized.add(raw.trim().toUpperCase(Locale.ROOT));
            }
        }
        return List.copyOf(normalized);
    }

    private List<DailyBar> fetchDailyBars(RestClient client, String symbol, LocalDate from, LocalDate to) {
        List<DailyBar> bars = new ArrayList<>();
        LocalDate cursor = from;
        while (!cursor.isAfter(to)) {
            LocalDate windowEnd = cursor.plusDays(MAX_DAYS_PER_REQUEST - 1L);
            if (windowEnd.isAfter(to)) {
                windowEnd = to;
            }
            long startTime = cursor.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            long endTime = windowEnd.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli() - 1;
            JsonNode response = client.get()
                    .uri(uriBuilder -> uriBuilder.path("/api/v3/klines")
                            .queryParam("symbol", symbol)
                            .queryParam("interval", "1d")
                            .queryParam("startTime", startTime)
                            .queryParam("endTime", endTime)
                            .queryParam("limit", MAX_DAYS_PER_REQUEST)
                            .build())
                    .retrieve()
                    .onStatus(HttpStatusCode::isError, (request, responseError) -> {
                        throw new IllegalArgumentException("Binance responded with " + responseError.getStatusCode());
                    })
                    .body(JsonNode.class);
            if (response == null || !response.isArray()) {
                throw new IllegalArgumentException("Binance returned an invalid kline response");
            }
            for (JsonNode row : response) {
                bars.add(new DailyBar(
                        symbol,
                        Instant.ofEpochMilli(row.get(0).asLong()).atZone(ZoneOffset.UTC).toLocalDate(),
                        decimal(row, 1), decimal(row, 2), decimal(row, 3), decimal(row, 4), decimal(row, 5)));
            }
            cursor = windowEnd.plusDays(1);
        }
        return bars;
    }

    private BigDecimal decimal(JsonNode row, int index) {
        return new BigDecimal(row.get(index).asText());
    }

    private void writeBars(List<DailyBar> bars) {
        if (bars.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
                """
                INSERT INTO symbol_daily_ohlcv
                    (symbol, trade_date, market_type, source, open, high, low, close, volume, tick_count, computed_at,
                     origin)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), 'EXCHANGE')
                ON CONFLICT (symbol, trade_date, source) DO UPDATE SET
                    origin = 'EXCHANGE',
                    market_type = EXCLUDED.market_type,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    tick_count = EXCLUDED.tick_count,
                    computed_at = NOW()
                """,
                bars,
                bars.size(),
                (ps, bar) -> {
                    ps.setString(1, bar.symbol());
                    ps.setObject(2, bar.tradeDate());
                    ps.setString(3, MARKET_TYPE);
                    ps.setString(4, SOURCE);
                    ps.setBigDecimal(5, bar.open());
                    ps.setBigDecimal(6, bar.high());
                    ps.setBigDecimal(7, bar.low());
                    ps.setBigDecimal(8, bar.close());
                    ps.setBigDecimal(9, bar.volume());
                    ps.setLong(10, 0L);
                });
    }

    private void pauseBetweenRequests() {
        try {
            Thread.sleep(REQUEST_INTERVAL_MS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Binance daily backfill was interrupted", ex);
        }
    }

    private String conciseMessage(Exception ex) {
        String message = ex.getMessage();
        return message == null || message.isBlank() ? ex.getClass().getSimpleName() : message;
    }

    public record BackfillResult(
            LocalDate from,
            LocalDate to,
            int requestedSymbolCount,
            int succeededSymbolCount,
            int writtenBarCount,
            List<String> failures
    ) {}

    private record DailyBar(
            String symbol,
            LocalDate tradeDate,
            BigDecimal open,
            BigDecimal high,
            BigDecimal low,
            BigDecimal close,
            BigDecimal volume
    ) {}
}
