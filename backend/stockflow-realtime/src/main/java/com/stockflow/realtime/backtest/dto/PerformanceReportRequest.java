package com.stockflow.realtime.backtest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

/** 대표 종목 성능 리포트 실행 조건. symbols 미지정 시 기본 10종을 사용한다. */
@Getter
public class PerformanceReportRequest {
    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate from;

    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate to;

    private List<String> symbols;
    private BigDecimal initialCash;
}
