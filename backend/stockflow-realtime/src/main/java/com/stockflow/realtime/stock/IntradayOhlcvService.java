package com.stockflow.realtime.stock;

import com.stockflow.realtime.stock.dto.IntradayOhlcvResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 분봉(인트라데이) OHLCV. market_ticks 원본 틱에서 요청 시점에 N분 단위로 집계한다.
 * time_bucket 등 TimescaleDB 전용 함수를 쓰지 않아 일반 PostgreSQL에서도 동작한다.
 */
@Service
@RequiredArgsConstructor
public class IntradayOhlcvService {

    /** 지원 봉 주기 → 버킷 길이(초). */
    private static final Map<String, Integer> INTERVAL_SECONDS = Map.of(
            "1m", 60,
            "5m", 300,
            "15m", 900,
            "1h", 3600
    );

    /** raw 틱 풀스캔을 막기 위한 조회 구간 상한. */
    private static final Duration MAX_RANGE = Duration.ofDays(7);
    private static final Duration DEFAULT_RANGE = Duration.ofDays(1);

    private final JdbcTemplate jdbcTemplate;

    public List<IntradayOhlcvResponse> getIntraday(String symbol, String interval, Instant from, Instant to) {
        Integer bucketSeconds = INTERVAL_SECONDS.get(interval);
        if (bucketSeconds == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "지원하지 않는 interval 입니다: " + interval + " (가능: " + INTERVAL_SECONDS.keySet() + ")");
        }

        Instant end = to != null ? to : Instant.now();
        Instant start = from != null ? from : end.minus(DEFAULT_RANGE);
        if (!start.isBefore(end)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "from은 to보다 앞서야 합니다.");
        }
        if (Duration.between(start, end).compareTo(MAX_RANGE) > 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "조회 구간은 최대 " + MAX_RANGE.toDays() + "일까지 가능합니다.");
        }

        String upperSymbol = symbol.toUpperCase();
        return jdbcTemplate.query(
                """
                SELECT bucket,
                       (array_agg(price ORDER BY ts ASC))[1]  AS open,
                       max(price)                              AS high,
                       min(price)                              AS low,
                       (array_agg(price ORDER BY ts DESC))[1] AS close,
                       sum(volume)                             AS volume,
                       count(*)                                AS tick_count
                FROM (
                    SELECT to_timestamp(floor(extract(epoch FROM ts) / ?) * ?) AS bucket,
                           price, volume, ts
                    FROM market_ticks
                    WHERE symbol = ? AND ts >= ? AND ts < ?
                ) t
                GROUP BY bucket
                ORDER BY bucket ASC
                """,
                (rs, rowNum) -> IntradayOhlcvResponse.builder()
                        .symbol(upperSymbol)
                        .time(rs.getTimestamp("bucket").toInstant())
                        .open(rs.getBigDecimal("open"))
                        .high(rs.getBigDecimal("high"))
                        .low(rs.getBigDecimal("low"))
                        .close(rs.getBigDecimal("close"))
                        .volume(rs.getBigDecimal("volume"))
                        .tickCount(rs.getLong("tick_count"))
                        .build(),
                bucketSeconds, bucketSeconds, upperSymbol,
                Timestamp.from(start), Timestamp.from(end)
        );
    }
}
