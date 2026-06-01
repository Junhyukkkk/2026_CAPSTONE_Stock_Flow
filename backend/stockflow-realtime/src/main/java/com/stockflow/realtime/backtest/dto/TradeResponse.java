package com.stockflow.realtime.backtest.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Builder
public class TradeResponse {
    private Integer seq;
    private LocalDate tradeDate;
    private String side;
    private BigDecimal price;
    private BigDecimal quantity;
    private BigDecimal cashAfter;
    private BigDecimal equityAfter;
    private BigDecimal pnlPct;
}
