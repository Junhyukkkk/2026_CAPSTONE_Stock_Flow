package com.stockflow.realtime.market.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;

@Getter
@Builder
public class StockRankItem {
    private String symbol;
    private String name;
    private String marketType;
    private BigDecimal currentPrice;
    private BigDecimal prevClose;
    private BigDecimal change;
    private BigDecimal changePercent;
}
