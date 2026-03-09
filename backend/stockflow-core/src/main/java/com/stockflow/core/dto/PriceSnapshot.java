package com.stockflow.core.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PriceSnapshot {

    private String symbol;

    private BigDecimal price;

    private BigDecimal volume;

    private String exchange;

    private Long timestamp;

    private BigDecimal change;          // 전일 대비 변동가

    private BigDecimal changePercent;   // 전일 대비 등락률(%)

    private String marketType;
}
