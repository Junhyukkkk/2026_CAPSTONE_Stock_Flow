package com.stockflow.realtime.controller;

import com.stockflow.realtime.controller.dto.IndicatorResponse;
import com.stockflow.realtime.service.IndicatorQueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@Tag(name = "Indicators", description = "기술적 지표 조회 API")
@RestController
@RequestMapping("/api/indicators")
@RequiredArgsConstructor
public class IndicatorController {

    private final IndicatorQueryService indicatorQueryService;

    @Operation(summary = "최신 지표 조회", description = "특정 심볼의 가장 최근 기술적 지표를 조회합니다.")
    @GetMapping("/{symbol}")
    public ResponseEntity<IndicatorResponse> getLatest(
            @Parameter(description = "종목 심볼 (예: AAPL, BTCUSDT)")
            @PathVariable String symbol) {

        return indicatorQueryService.getLatest(symbol)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @Operation(summary = "지표 히스토리 조회", description = "특정 심볼의 최근 N일 지표를 조회합니다.")
    @GetMapping("/{symbol}/history")
    public ResponseEntity<List<IndicatorResponse>> getHistory(
            @Parameter(description = "종목 심볼")
            @PathVariable String symbol,
            @Parameter(description = "조회할 일수 (기본값: 30)")
            @RequestParam(defaultValue = "30") int days) {

        List<IndicatorResponse> history = indicatorQueryService.getHistory(symbol, days);

        if (history.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(history);
    }

    @Operation(summary = "기간별 지표 조회", description = "특정 심볼의 지정 기간 지표를 조회합니다.")
    @GetMapping("/{symbol}/range")
    public ResponseEntity<List<IndicatorResponse>> getByDateRange(
            @Parameter(description = "종목 심볼")
            @PathVariable String symbol,
            @Parameter(description = "시작일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @Parameter(description = "종료일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {

        List<IndicatorResponse> results = indicatorQueryService.getByDateRange(symbol, from, to);

        if (results.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(results);
    }
}
