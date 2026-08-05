package com.stockflow.realtime.batch.service;

import com.stockflow.realtime.service.RedisPriceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 전일 종가 동기화 서비스.
 *
 * symbol_daily_ohlcv(일봉)의 심볼별 최신 종가를 Redis(price:prev-close:{symbol})에 적재한다.
 * 실시간 경로(RedisPriceService)는 이 값을 읽어 등락률을 계산하는데,
 * 이 값을 채우는 주체가 그동안 없어 등락률이 항상 0으로 계산되던 문제를 해결한다.
 *
 * 일봉 집계(dailyOhlcvJob) 직후에 실행되어야 갓 마감된 종가가 반영된다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PrevCloseSyncService {

    private final JdbcTemplate jdbcTemplate;
    private final RedisPriceService redisPriceService;

    /**
     * 심볼별 최신 일봉 종가를 조회해 Redis 전일 종가로 적재한다.
     *
     * @return 적재한 종목 수
     */
    public int syncFromDailyOhlcv() {
        // DISTINCT ON 으로 심볼별 가장 최근 trade_date 의 종가만 뽑는다.
        String sql = """
                SELECT DISTINCT ON (symbol) symbol, close
                FROM symbol_daily_ohlcv
                ORDER BY symbol, trade_date DESC
                """;

        Map<String, BigDecimal> closes = new LinkedHashMap<>();
        jdbcTemplate.query(sql, rs -> {
            closes.put(rs.getString("symbol"), rs.getBigDecimal("close"));
        });

        if (closes.isEmpty()) {
            log.warn("전일 종가 동기화: symbol_daily_ohlcv 에 데이터가 없어 적재를 건너뜀");
            return 0;
        }

        int loaded = redisPriceService.loadPreviousCloses(closes);
        log.info("전일 종가 동기화 완료: {}개 종목", loaded);
        return loaded;
    }
}
