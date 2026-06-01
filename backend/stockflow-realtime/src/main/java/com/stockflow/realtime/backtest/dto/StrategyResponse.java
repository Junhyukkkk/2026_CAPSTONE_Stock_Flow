package com.stockflow.realtime.backtest.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Getter
@Builder
public class StrategyResponse {
    private Long id;
    private String name;
    private String symbol;
    private String strategyType;
    private Map<String, Object> params;
    private BigDecimal initialCash;
    private Instant createdAt;
    private Instant updatedAt;
}
