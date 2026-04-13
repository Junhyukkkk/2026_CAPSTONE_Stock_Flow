package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 틱 저장 시 {@code instruments} 마스터를 {@code register_instrument} 로 동기화.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InstrumentRegistryService {

    private final JdbcTemplate jdbcTemplate;

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
            try {
                jdbcTemplate.update(
                        "SELECT register_instrument(?, ?, ?, ?)",
                        key,
                        t.getMarketType().trim(),
                        t.getExchange().trim(),
                        key
                );
            } catch (Exception e) {
                log.warn("register_instrument failed: symbol={}", key, e);
            }
        }
    }
}
