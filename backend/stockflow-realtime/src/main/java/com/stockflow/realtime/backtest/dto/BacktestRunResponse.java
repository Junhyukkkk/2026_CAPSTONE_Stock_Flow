package com.stockflow.realtime.backtest.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;

/**
 * 백테스트 실행 요약 결과.
 */
@Getter
@Builder
public class BacktestRunResponse {
    private Long id;
    private Long strategyId;
    private String symbol;
    private String strategyType;
    private Map<String, Object> params;
    private LocalDate fromDate;
    private LocalDate toDate;
    private BigDecimal initialCash;
    private BigDecimal finalEquity;
    private BigDecimal totalReturnPct;
    private BigDecimal cagrPct;
    private BigDecimal mddPct;
    private Integer tradeCount;
    private BigDecimal winRatePct;
    private Integer barCount;
    private String status;
    private Instant createdAt;
}
