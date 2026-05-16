package com.stockflow.realtime.stock.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;

@Getter
@Builder
public class InstrumentResponse {
    private String symbol;
    private String name;
    private String marketType;
    private String exchange;
    private boolean active;
    private BigDecimal currentPrice;
    private BigDecimal change;
    private BigDecimal changePercent;
    private Instant lastSeenAt;
}
