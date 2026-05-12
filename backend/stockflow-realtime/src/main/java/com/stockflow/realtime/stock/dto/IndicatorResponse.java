package com.stockflow.realtime.stock.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Builder
public class IndicatorResponse {
    private String symbol;
    private LocalDate date;
    private BigDecimal ma5;
    private BigDecimal ma20;
    private BigDecimal ma60;
    private BigDecimal rsi14;
    private BigDecimal macd;
    private BigDecimal macdSignal;
    private BigDecimal macdHist;
}
