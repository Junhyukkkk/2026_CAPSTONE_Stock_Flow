package com.stockflow.realtime.storage;

import com.stockflow.core.dto.NormalizedTradeDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class InstrumentRegistryServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private InstrumentRegistryService instrumentRegistryService;

    @Test
    void registerDistinctFromTrades_callsOncePerSymbol() {
        long ts = 1_700_000_000_000L;
        NormalizedTradeDTO a = NormalizedTradeDTO.builder()
                .source("BINANCE")
                .symbol("btcusdt")
                .tradeId("1")
                .price(BigDecimal.ONE)
                .volume(BigDecimal.ONE)
                .exchange("BINANCE")
                .timestamp(ts)
                .receivedAt(ts)
                .marketType("CRYPTO")
                .build();
        NormalizedTradeDTO b = NormalizedTradeDTO.builder()
                .source("BINANCE")
                .symbol("BTCUSDT")
                .tradeId("2")
                .price(BigDecimal.TWO)
                .volume(BigDecimal.ONE)
                .exchange("BINANCE")
                .timestamp(ts + 1)
                .receivedAt(ts + 1)
                .marketType("CRYPTO")
                .build();

        instrumentRegistryService.registerDistinctFromTrades(List.of(a, b));

        verify(jdbcTemplate, times(1)).update(
                eq("SELECT register_instrument(?, ?, ?, ?)"),
                eq("BTCUSDT"),
                eq("CRYPTO"),
                eq("BINANCE"),
                eq("BTCUSDT")
        );
    }

    @Test
    void registerDistinctFromTrades_skipsEmpty() {
        instrumentRegistryService.registerDistinctFromTrades(List.of());
        verifyNoInteractions(jdbcTemplate);
    }
}
