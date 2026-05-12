package com.stockflow.realtime.batch.item;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DailyIndicatorItem {
    private String symbol;
    private LocalDate tradeDate;
    private BigDecimal ma5;
    private BigDecimal ma20;
    private BigDecimal ma60;
    private BigDecimal rsi14;
    private BigDecimal macd;
    private BigDecimal macdSignal;
    private BigDecimal macdHist;
}
