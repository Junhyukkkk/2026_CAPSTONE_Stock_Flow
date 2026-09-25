package com.stockflow.realtime.backtest.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/** BTCUSDT 1분봉 즉석 백테스트 요청. */
@Getter
public class IntradayRunRequest {

    @NotBlank
    private String symbol;

    /** BUY_AND_HOLD | MA_CROSSOVER */
    @NotBlank
    private String strategyType;

    /** MA_CROSSOVER: shortPeriod, longPeriod. */
    private Map<String, Object> params;

    /** 미지정 시 10000 USDT. */
    private BigDecimal initialCash;

    /** 미지정 시 10 bps. */
    private BigDecimal feeBps;

    /** 미지정 시 5 bps. */
    private BigDecimal slippageBps;

    @NotNull
    private Instant from;

    @NotNull
    private Instant to;
}
