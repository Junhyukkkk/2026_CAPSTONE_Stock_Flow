package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.config.OptimizationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class InstrumentRegistryServiceTest {

    private static final String REGISTER_SQL = "SELECT register_instrument(?, ?, ?, ?)";

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private OptimizationProperties opt;

    @InjectMocks
    private InstrumentRegistryService instrumentRegistryService;

    @BeforeEach
    void setUp() {
        // instrumentCache는 registerDistinctFromTrades_skipsEmpty 에서는 호출되지 않으므로 lenient.
        lenient().when(opt.isInstrumentCache()).thenReturn(false);
    }

    @Test
    @SuppressWarnings("unchecked")
    void registerDistinctFromTrades_callsOncePerSymbol() {
        long ts = 1_700_000_000_000L;
        NormalizedTradeDTO a = trade("btcusdt", "1", ts);
        NormalizedTradeDTO b = trade("BTCUSDT", "2", ts + 1);

        instrumentRegistryService.registerDistinctFromTrades(List.of(a, b));

        // SELECT 는 결과셋을 돌려주므로 반드시 query() 로 호출해야 한다.
        // (update() 로 부르면 "A result was returned when none was expected" 가 매번 발생 — 보고서 문제 ②)
        verify(jdbcTemplate, times(1)).query(
                eq(REGISTER_SQL),
                any(ResultSetExtractor.class),
                eq("BTCUSDT"),
                eq("CRYPTO"),
                eq("BINANCE"),
                eq("BTCUSDT")
        );
        // update() 등 다른 JdbcTemplate 호출이 없어야 한다
        verifyNoMoreInteractions(jdbcTemplate);
    }

    @Test
    void registerDistinctFromTrades_skipsEmpty() {
        instrumentRegistryService.registerDistinctFromTrades(List.of());
        verifyNoInteractions(jdbcTemplate);
    }

    private static NormalizedTradeDTO trade(String symbol, String tradeId, long ts) {
        return NormalizedTradeDTO.builder()
                .source("BINANCE")
                .symbol(symbol)
                .tradeId(tradeId)
                .price(BigDecimal.ONE)
                .volume(BigDecimal.ONE)
                .exchange("BINANCE")
                .timestamp(ts)
                .receivedAt(ts)
                .marketType("CRYPTO")
                .build();
    }
}
