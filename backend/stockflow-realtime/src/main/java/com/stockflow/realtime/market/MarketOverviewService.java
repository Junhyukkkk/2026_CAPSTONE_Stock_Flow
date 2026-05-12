package com.stockflow.realtime.market;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.market.dto.MarketOverviewResponse;
import com.stockflow.realtime.market.dto.StockRankItem;
import com.stockflow.realtime.service.RedisPriceService;
import com.stockflow.realtime.stock.InstrumentRepository;
import com.stockflow.realtime.stock.InstrumentRepository.InstrumentRow;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class MarketOverviewService {

    private final InstrumentRepository instrumentRepository;
    private final RedisPriceService redisPriceService;
    private final JdbcTemplate jdbcTemplate;

    private static final int RANK_SIZE = 10;

    public MarketOverviewResponse getOverview(String marketType) {
        List<InstrumentRow> instruments = instrumentRepository.findAll(marketType, true);
        if (instruments.isEmpty()) {
            return MarketOverviewResponse.builder()
                    .totalActive(0).withRealtimePrice(0)
                    .topGainers(List.of()).topLosers(List.of())
                    .build();
        }

        List<String> symbols = instruments.stream().map(InstrumentRow::symbol).toList();
        Map<String, BigDecimal> prevCloseMap = fetchPrevCloses(symbols);

        List<StockRankItem> items = new ArrayList<>();
        int withRealtimePrice = 0;

        for (InstrumentRow row : instruments) {
            PriceSnapshot snapshot = redisPriceService.getLatestPrice(row.symbol());
            BigDecimal prevClose = prevCloseMap.get(row.symbol());

            if (snapshot == null && prevClose == null) continue;

            BigDecimal currentPrice = snapshot != null ? snapshot.getPrice() : prevClose;
            BigDecimal change = snapshot != null ? snapshot.getChange() : BigDecimal.ZERO;
            BigDecimal changePercent = snapshot != null ? snapshot.getChangePercent() : BigDecimal.ZERO;

            if (snapshot != null) withRealtimePrice++;

            items.add(StockRankItem.builder()
                    .symbol(row.symbol())
                    .name(row.name())
                    .marketType(row.marketType())
                    .currentPrice(currentPrice)
                    .prevClose(prevClose)
                    .change(change)
                    .changePercent(changePercent)
                    .build());
        }

        List<StockRankItem> topGainers = items.stream()
                .sorted(Comparator.comparing(StockRankItem::getChangePercent).reversed())
                .limit(RANK_SIZE)
                .toList();

        List<StockRankItem> topLosers = items.stream()
                .sorted(Comparator.comparing(StockRankItem::getChangePercent))
                .limit(RANK_SIZE)
                .toList();

        return MarketOverviewResponse.builder()
                .totalActive(instruments.size())
                .withRealtimePrice(withRealtimePrice)
                .topGainers(topGainers)
                .topLosers(topLosers)
                .build();
    }

    private Map<String, BigDecimal> fetchPrevCloses(List<String> symbols) {
        if (symbols.isEmpty()) return Map.of();

        String placeholders = symbols.stream().map(s -> "?").collect(Collectors.joining(", "));
        String sql = """
                SELECT DISTINCT ON (symbol) symbol, close
                FROM symbol_daily_ohlcv
                WHERE symbol IN (%s)
                ORDER BY symbol, trade_date DESC
                """.formatted(placeholders);

        return jdbcTemplate.query(sql,
                (rs, rowNum) -> Map.entry(rs.getString("symbol"), rs.getBigDecimal("close")),
                symbols.toArray()
        ).stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
