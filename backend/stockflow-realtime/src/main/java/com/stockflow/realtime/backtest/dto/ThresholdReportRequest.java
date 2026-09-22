package com.stockflow.realtime.backtest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/** 선택한 예측 모델의 신호 기준 계수 비교 요청. */
@Getter
public class ThresholdReportRequest {
    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate from;

    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate to;

    @NotBlank
    private String model;

    private List<String> symbols;
    private BigDecimal initialCash;
}
