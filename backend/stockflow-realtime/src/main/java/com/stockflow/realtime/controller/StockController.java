package com.stockflow.realtime.controller;

import com.stockflow.realtime.stock.IndicatorHistoryService;
import com.stockflow.realtime.stock.InstrumentService;
import com.stockflow.realtime.stock.OhlcvHistoryService;
import com.stockflow.realtime.stock.dto.IndicatorResponse;
import com.stockflow.realtime.stock.dto.InstrumentCreateRequest;
import com.stockflow.realtime.stock.dto.InstrumentResponse;
import com.stockflow.realtime.stock.dto.InstrumentUpdateRequest;
import com.stockflow.realtime.stock.dto.OhlcvResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/stocks")
@RequiredArgsConstructor
@Tag(name = "Stock", description = "종목 관리 및 시세 조회 API")
public class StockController {

    private final InstrumentService instrumentService;
    private final OhlcvHistoryService ohlcvHistoryService;
    private final IndicatorHistoryService indicatorHistoryService;

    @GetMapping
    @Operation(summary = "종목 목록 조회", description = "등록된 종목 목록과 실시간 가격을 조회합니다.")
    public List<InstrumentResponse> list(
            @Parameter(description = "마켓 타입 필터 (STOCK, CRYPTO)")
            @RequestParam(required = false) String marketType,
            @Parameter(description = "활성 종목만 조회")
            @RequestParam(defaultValue = "true") boolean activeOnly
    ) {
        return instrumentService.list(marketType, activeOnly);
    }

    @GetMapping("/{symbol}")
    @Operation(summary = "종목 상세 조회", description = "특정 종목의 상세 정보와 실시간 가격을 조회합니다.")
    public ResponseEntity<InstrumentResponse> getBySymbol(@PathVariable String symbol) {
        return instrumentService.getBySymbol(symbol)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    @Operation(summary = "종목 등록", description = "새로운 종목을 등록합니다. 이미 존재하는 심볼은 정보가 업데이트됩니다.")
    public ResponseEntity<InstrumentResponse> create(@Valid @RequestBody InstrumentCreateRequest request) {
        return ResponseEntity.ok(instrumentService.create(request));
    }

    @PutMapping("/{symbol}")
    @Operation(summary = "종목 정보 수정", description = "종목의 이름 또는 거래소 정보를 수정합니다.")
    public ResponseEntity<InstrumentResponse> update(
            @PathVariable String symbol,
            @RequestBody InstrumentUpdateRequest request
    ) {
        return instrumentService.update(symbol, request)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/{symbol}")
    @Operation(summary = "종목 비활성화", description = "종목을 비활성화합니다 (소프트 삭제).")
    public ResponseEntity<Void> deactivate(@PathVariable String symbol) {
        return instrumentService.deactivate(symbol)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    @GetMapping("/{symbol}/ohlcv")
    @Operation(summary = "일봉 OHLCV 조회", description = "일봉 시가/고가/저가/종가/거래량 데이터를 조회합니다.")
    public List<OhlcvResponse> getOhlcv(
            @PathVariable String symbol,
            @Parameter(description = "시작일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @Parameter(description = "종료일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        return ohlcvHistoryService.getOhlcv(symbol, from, to);
    }

    @GetMapping("/{symbol}/indicators")
    @Operation(summary = "기술적 지표 조회", description = "MA5/MA20/MA60, RSI14, MACD 지표를 조회합니다.")
    public List<IndicatorResponse> getIndicators(
            @PathVariable String symbol,
            @Parameter(description = "시작일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @Parameter(description = "종료일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to
    ) {
        return indicatorHistoryService.getIndicators(symbol, from, to);
    }
}
