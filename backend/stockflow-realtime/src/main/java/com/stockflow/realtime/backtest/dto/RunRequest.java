package com.stockflow.realtime.backtest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;

/**
 * 저장하지 않고 즉석으로 실행하는 ad-hoc 백테스트 요청.
 */
@Getter
public class RunRequest {

    @NotBlank
    private String symbol;

    /** BUY_AND_HOLD | MA_CROSSOVER | RSI */
    @NotBlank
    private String strategyType;

    private Map<String, Object> params;

    /** 미지정 시 10000. */
    private BigDecimal initialCash;

    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate from;

    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate to;
}
