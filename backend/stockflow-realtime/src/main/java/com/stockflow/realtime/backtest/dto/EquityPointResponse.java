package com.stockflow.realtime.backtest.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Builder
public class EquityPointResponse {
    private LocalDate tradeDate;
    private BigDecimal equity;
    private BigDecimal drawdownPct;
}
