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

    // 볼린저 밴드
    private BigDecimal bbUpper;     // 상단 밴드 (MA20 + 2σ)
    private BigDecimal bbLower;     // 하단 밴드 (MA20 - 2σ)

    // 스토캐스틱
    private BigDecimal stochK;      // %K (14일)
    private BigDecimal stochD;      // %D (%K의 3일 SMA)

    // ATR
    private BigDecimal atr14;       // 14일 ATR

    // OBV
    private Long obv;               // On Balance Volume
}
