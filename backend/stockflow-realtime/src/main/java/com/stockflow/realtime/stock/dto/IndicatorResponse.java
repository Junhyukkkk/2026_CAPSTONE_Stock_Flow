package com.stockflow.realtime.stock.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IndicatorResponse {
    private String symbol;
    private LocalDate tradeDate;

    // 이동평균
    private BigDecimal ma5;
    private BigDecimal ma20;
    private BigDecimal ma60;

    // RSI
    private BigDecimal rsi14;

    // MACD
    private BigDecimal macd;
    private BigDecimal macdSignal;
    private BigDecimal macdHist;

    // 볼린저 밴드
    private BigDecimal bbUpper;
    private BigDecimal bbMiddle;
    private BigDecimal bbLower;

    // 스토캐스틱
    private BigDecimal stochK;
    private BigDecimal stochD;

    // ATR
    private BigDecimal atr14;

    // OBV
    private Long obv;
}
