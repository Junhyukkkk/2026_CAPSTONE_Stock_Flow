package com.stockflow.realtime.stock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 같은 심볼이 여러 출처(ALPACA 실데이터, SIMULATOR, BINANCE)로 ohlcv_1m 에 저장될 때,
 * 가장 최근에 봉이 들어온 출처를 서버에서 결정한다. 화면(instruments.exchange)은 마지막 기록자가 덮어써서 믿을 수 없다.
 *
 * <p>최근 3일 범위로 한정해 최신 청크만 훑는다(HDD 서버에서 전체 스캔 방지). 그 안에 봉이 없으면 비어 있고,
 * 호출자는 출처 필터 없이 기존 동작을 유지한다.
 */
@Component
public class SymbolSourceResolver {

    private static final String SQL = """
            SELECT source FROM ohlcv_1m
            WHERE symbol = ? AND bucket >= now() - interval '3 days'
            GROUP BY source
            ORDER BY max(bucket) DESC
            LIMIT 1
            """;

    private static final Duration CACHE_TTL = Duration.ofSeconds(30);

    private record Cached(String source, long expiresAtMillis) {
    }

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;
    /** 찾은 출처만 담는다. 비어 있는 결과는 캐시하지 않아 새로 들어온 심볼이 바로 반영된다. */
    private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();

    @Autowired
    public SymbolSourceResolver(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, Clock.systemUTC());
    }

    SymbolSourceResolver(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    public Optional<String> latestSource(String symbol) {
        if (symbol == null || symbol.isBlank()) {
            return Optional.empty();
        }
        String key = symbol.trim().toUpperCase(Locale.ROOT);
        long now = clock.millis();
        Cached cached = cache.get(key);
        if (cached != null && cached.expiresAtMillis() > now) {
            return Optional.of(cached.source());
        }
        List<String> sources = jdbcTemplate.queryForList(SQL, String.class, key);
        if (sources.isEmpty() || sources.get(0) == null) {
            cache.remove(key);
            return Optional.empty();
        }
        cache.put(key, new Cached(sources.get(0), now + CACHE_TTL.toMillis()));
        return Optional.of(sources.get(0));
    }
}
