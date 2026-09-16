package com.stockflow.realtime.backtest.dto;

import lombok.Builder;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;

/** 예측 기반 백테스트의 날짜별 모델 판단 근거. */
@Getter
@Builder
public class PredictionPointResponse {
    private LocalDate signalDate;
    private LocalDate executionDate;
    private BigDecimal referencePrice;
    private BigDecimal predictedPrice;
    private BigDecimal expectedReturnPct;
    private BigDecimal thresholdPct;
    private String signal;
}
