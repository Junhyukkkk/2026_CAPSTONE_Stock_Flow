package com.stockflow.realtime.stock;

import com.stockflow.core.dto.PriceSnapshot;
import com.stockflow.realtime.service.RedisPriceService;
import com.stockflow.realtime.stock.InstrumentRepository.InstrumentRow;
import com.stockflow.realtime.stock.dto.InstrumentCreateRequest;
import com.stockflow.realtime.stock.dto.InstrumentResponse;
import com.stockflow.realtime.stock.dto.InstrumentUpdateRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class InstrumentService {

    private final InstrumentRepository repository;
    private final RedisPriceService redisPriceService;

    public List<InstrumentResponse> list(String marketType, boolean activeOnly) {
        return repository.findAll(marketType, activeOnly).stream()
                .map(row -> enrich(row, redisPriceService.getLatestPrice(row.symbol())))
                .toList();
    }

    public Optional<InstrumentResponse> getBySymbol(String symbol) {
        return repository.findBySymbol(symbol.toUpperCase())
                .map(row -> enrich(row, redisPriceService.getLatestPrice(row.symbol())));
    }

    public InstrumentResponse create(InstrumentCreateRequest req) {
        repository.upsert(req.getSymbol(), req.getMarketType(), req.getExchange(), req.getName());
        return getBySymbol(req.getSymbol()).orElseThrow();
    }

    public Optional<InstrumentResponse> update(String symbol, InstrumentUpdateRequest req) {
        Optional<InstrumentResponse> existing = getBySymbol(symbol);
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        if (req.getName() != null || req.getExchange() != null) {
            repository.update(symbol, req.getName(), req.getExchange());
        }
        return getBySymbol(symbol);
    }

    public boolean deactivate(String symbol) {
        return repository.setActive(symbol.toUpperCase(), false) > 0;
    }

    private InstrumentResponse enrich(InstrumentRow row, PriceSnapshot snapshot) {
        return InstrumentResponse.builder()
                .symbol(row.symbol())
                .name(row.name())
                .marketType(row.marketType())
                .exchange(row.exchange())
                .active(row.active())
                .currentPrice(snapshot != null ? snapshot.getPrice() : null)
                .change(snapshot != null ? snapshot.getChange() : null)
                .changePercent(snapshot != null ? snapshot.getChangePercent() : null)
                .lastSeenAt(row.lastSeenAt())
                .build();
    }
}
