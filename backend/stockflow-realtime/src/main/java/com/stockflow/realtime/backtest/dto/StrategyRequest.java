package com.stockflow.realtime.backtest.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Getter;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 백테스트 전략 생성/수정 요청.
 */
@Getter
public class StrategyRequest {

    @NotBlank
    private String name;

    @NotBlank
    private String symbol;

    /** BUY_AND_HOLD | MA_CROSSOVER | RSI */
    @NotBlank
    private String strategyType;

    /** 전략 타입별 파라미터 (예: {"shortPeriod":5,"longPeriod":20}). */
    private Map<String, Object> params;

    /** 초기 자본. 미지정 시 10000. */
    private BigDecimal initialCash;
}
