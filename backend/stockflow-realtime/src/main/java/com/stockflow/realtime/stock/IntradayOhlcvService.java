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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 분봉(인트라데이) OHLCV. 1분봉 저장소(ohlcv_1m)를 N분 단위로 묶어 반환한다.
 *
 * <p>ohlcv_1m 은 market-data-sync 가 1분마다 채우므로 실시간보다 2~3분 늦다. 그래서 구간 끝 30분 안에서
 * 마지막 저장 캔들 이후 분만 원본 틱(market_ticks)에서 보충해 최근 봉이 비지 않게 한다.
 * time_bucket 등 TimescaleDB 전용 함수를 쓰지 않아 일반 PostgreSQL에서도 동작한다.
 */
@Service
@RequiredArgsConstructor
public class IntradayOhlcvService {

    /** 지원 봉 주기 → 버킷 길이(초). 모두 1분의 배수여야 한다. */
    private static final Map<String, Integer> INTERVAL_SECONDS = Map.of(
            "1m", 60,
            "5m", 300,
            "15m", 900,
            "1h", 3600
    );

    /** 과도한 응답을 막기 위한 조회 구간 상한. */
    private static final Duration MAX_RANGE = Duration.ofDays(7);
    private static final Duration DEFAULT_RANGE = Duration.ofDays(1);
    /**
     * 원본 틱 보충은 구간 끝에서 이만큼만 본다. ohlcv_1m 은 2~3분 늦을 뿐이고 과거 구간은 이미 채워져 있다.
     * 운영 실측: 고정 하한이 없으면 25초 초과(타임아웃), 30분 하한이면 11ms.
     */
    private static final Duration TOP_UP_WINDOW = Duration.ofMinutes(30);

    /** SQL 의 {@code @SRC@} 자리에 들어갈 출처 조건. 값은 항상 바인드 파라미터로 넘긴다. */
    private static final String SOURCE_PREDICATE = " AND source = ?";

    private static final String SQL = """
            WITH last_candle AS (
                SELECT coalesce(max(bucket) + interval '1 minute', CAST(? AS timestamptz)) AS cutoff
                FROM ohlcv_1m
                WHERE symbol = ?@SRC@ AND bucket >= ? AND bucket < ?
            ), minutes AS (
                SELECT bucket, open, high, low, close, volume, trade_count
                FROM ohlcv_1m
                WHERE symbol = ?@SRC@ AND bucket >= ? AND bucket < ?
                UNION ALL
                SELECT date_trunc('minute', t.ts)                AS bucket,
                       (array_agg(t.price ORDER BY t.ts ASC))[1]  AS open,
                       max(t.price)                               AS high,
                       min(t.price)                               AS low,
                       (array_agg(t.price ORDER BY t.ts DESC))[1] AS close,
                       sum(t.volume)                              AS volume,
                       count(*)                                   AS trade_count
                FROM market_ticks t, last_candle l
                -- 고정 하한(? = 보충 시작)이 있어야 청크 제외가 된다. cutoff 만 쓰면 원본 틱 전체(83GB)를 훑는다.
                WHERE t.symbol = ?@SRC@ AND t.ts >= ? AND t.ts >= l.cutoff AND t.ts < ?
                GROUP BY 1
            )
            SELECT to_timestamp(floor(extract(epoch FROM bucket) / ?) * ?) AS bucket,
                   (array_agg(open ORDER BY bucket ASC))[1]  AS open,
                   max(high)                                  AS high,
                   min(low)                                   AS low,
                   (array_agg(close ORDER BY bucket DESC))[1] AS close,
                   sum(volume)                                AS volume,
                   -- 실시간(LIVE) 캔들은 체결 수를 모른다. 하나라도 모르면 0 대신 NULL 로 둔다.
                   -- sum(bigint) 는 numeric 이라 JDBC 가 Long 으로 바꾸지 못한다 → bigint 로 캐스팅.
                   CASE WHEN count(trade_count) = count(*) THEN sum(trade_count)::bigint END AS tick_count
            FROM minutes
            GROUP BY 1
            ORDER BY 1 ASC
            """;

    private final JdbcTemplate jdbcTemplate;

    public List<IntradayOhlcvResponse> getIntraday(String symbol, String interval, Instant from, Instant to) {
        return getIntraday(symbol, interval, from, to, null);
    }

    /**
     * @param source 데이터 출처(ohlcv_1m/market_ticks 의 source, 예: ALPACA·SIMULATOR·BINANCE). null/blank 면 출처 무관.
     *               같은 심볼이 여러 출처로 저장될 수 있어, 지정하지 않으면 출처가 한 캔들에 섞인다.
     */
    public List<IntradayOhlcvResponse> getIntraday(String symbol, String interval, Instant from, Instant to,
                                                   String source) {
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
        Timestamp startTs = Timestamp.from(start);
        Timestamp endTs = Timestamp.from(end);
        Instant topUpFrom = end.minus(TOP_UP_WINDOW);
        Timestamp topUpFromTs = Timestamp.from(topUpFrom.isAfter(start) ? topUpFrom : start);
        String upperSource = source == null || source.isBlank() ? null : source.trim().toUpperCase();
        String sql = SQL.replace("@SRC@", upperSource == null ? "" : SOURCE_PREDICATE);
        List<Object> args = new ArrayList<>();
        args.add(startTs);
        addSymbolArgs(args, upperSymbol, upperSource, startTs, endTs);      // last_candle 는 cutoff 기본값이 앞에 온다
        addSymbolArgs(args, upperSymbol, upperSource, startTs, endTs);      // ohlcv_1m
        addSymbolArgs(args, upperSymbol, upperSource, topUpFromTs, endTs);  // market_ticks 보충
        args.add(bucketSeconds);
        args.add(bucketSeconds);
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> IntradayOhlcvResponse.builder()
                        .symbol(upperSymbol)
                        .time(rs.getTimestamp("bucket").toInstant())
                        .open(rs.getBigDecimal("open"))
                        .high(rs.getBigDecimal("high"))
                        .low(rs.getBigDecimal("low"))
                        .close(rs.getBigDecimal("close"))
                        .volume(rs.getBigDecimal("volume"))
                        .tickCount(rs.getObject("tick_count", Long.class))
                        .build(),
                args.toArray()
        );
    }

    private static void addSymbolArgs(List<Object> args, String symbol, String source, Timestamp from, Timestamp to) {
        args.add(symbol);
        if (source != null) {
            args.add(source);
        }
        args.add(from);
        args.add(to);
    }
}
