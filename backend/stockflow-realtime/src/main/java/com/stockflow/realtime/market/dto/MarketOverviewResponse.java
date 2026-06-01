package com.stockflow.realtime.market.dto;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
public class MarketOverviewResponse {
    private int totalActive;
    private int withRealtimePrice;
    private List<StockRankItem> topGainers;
    private List<StockRankItem> topLosers;
}
