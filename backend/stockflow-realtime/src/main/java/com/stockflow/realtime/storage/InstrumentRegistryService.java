package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.config.OptimizationProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 틱 저장 시 {@code instruments} 마스터를 {@code register_instrument} 로 동기화.
 *
 * register_instrument 는 ON CONFLICT DO UPDATE 이므로 호출할 때마다 write 가 발생한다.
 * 캐시가 없으면 배치마다 같은 심볼을 재등록해 소수의 행에 UPDATE 가 누적되고,
 * 행 잠금 경합과 autovacuum 부하로 이어진다.
 * {@code stockflow.opt.instrument-cache=true} 로 켜면 심볼별 마지막 등록 시각을
 * 메모리에 두고 갱신 주기가 지난 경우에만 호출한다.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InstrumentRegistryService {

    private final JdbcTemplate jdbcTemplate;
    private final OptimizationProperties opt;

    /** symbol -> 마지막으로 register_instrument 를 호출한 시각(epoch ms) */
    private final Map<String, Long> lastRegisteredAt = new ConcurrentHashMap<>();

    public void registerDistinctFromTrades(List<NormalizedTradeDTO> trades) {
        if (trades == null || trades.isEmpty()) {
            return;
        }
        Set<String> seen = new HashSet<>();
        for (NormalizedTradeDTO t : trades) {
            if (t.getSymbol() == null || t.getMarketType() == null || t.getExchange() == null) {
                continue;
            }
            String sym = t.getSymbol().trim();
            if (sym.isEmpty()) {
                continue;
            }
            String key = sym.toUpperCase(Locale.ROOT);
            if (!seen.add(key)) {
                continue;
            }
            if (opt.isInstrumentCache() && !needsRegister(key)) {
                continue;
            }
            try {
                register(key, t.getMarketType().trim(), t.getExchange().trim());
                if (opt.isInstrumentCache()) {
                    lastRegisteredAt.put(key, System.currentTimeMillis());
                }
            } catch (Exception e) {
                log.warn("register_instrument failed: symbol={}", key, e);
            }
        }
    }

    private static final String REGISTER_SQL = "SELECT register_instrument(?, ?, ?, ?)";

    /**
     * register_instrument 실행.
     *
     * SELECT 는 함수가 void 를 반환해도 결과셋을 돌려주므로 update() 로 호출하면
     * "A result was returned when none was expected" 예외가 매 호출마다 발생한다.
     * (함수 자체는 실행되므로 데이터는 반영되고 예외만 삼켜져 왔다.)
     */
    private void register(String symbol, String marketType, String exchange) {
        if (!opt.isInstrumentRegistryFix()) {
            jdbcTemplate.update(REGISTER_SQL, symbol, marketType, exchange, symbol);
            return;
        }
        jdbcTemplate.query(REGISTER_SQL, rs -> null, symbol, marketType, exchange, symbol);
    }

    /**
     * 갱신 주기가 지났는지 확인한다.
     * last_seen_at 을 계속 쓸 수 있도록 완전히 건너뛰지 않고 주기적으로만 호출한다.
     */
    private boolean needsRegister(String symbol) {
        Long last = lastRegisteredAt.get(symbol);
        return last == null
                || System.currentTimeMillis() - last >= opt.getInstrumentCacheRefreshMs();
    }
}
