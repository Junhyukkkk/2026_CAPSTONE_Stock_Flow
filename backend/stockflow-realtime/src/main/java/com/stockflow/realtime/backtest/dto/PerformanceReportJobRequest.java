package com.stockflow.realtime.backtest.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

import java.math.BigDecimal;
import java.time.LocalDate;

/** 전체 암호화폐 성능 리포트 작업의 실행 조건. */
@Getter
public class PerformanceReportJobRequest {
    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate from;

    @NotNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate to;

    private BigDecimal initialCash;

    @Min(50)
    @Max(500)
    private Integer minimumHistoryDays = 50;
}
