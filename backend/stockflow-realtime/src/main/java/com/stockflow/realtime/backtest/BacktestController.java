package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.EquityPointResponse;
import com.stockflow.realtime.backtest.dto.RunRequest;
import com.stockflow.realtime.backtest.dto.StrategyRequest;
import com.stockflow.realtime.backtest.dto.StrategyResponse;
import com.stockflow.realtime.backtest.dto.TradeResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/backtest")
@RequiredArgsConstructor
@Tag(name = "Backtest", description = "백테스팅 전략 관리 및 실행 API")
public class BacktestController {

    private final BacktestStrategyService strategyService;
    private final BacktestRunService runService;

    // ----- 전략 CRUD -----

    @PostMapping("/strategies")
    @Operation(summary = "전략 생성", description = "백테스트 전략을 생성합니다. (BUY_AND_HOLD, MA_CROSSOVER, RSI)")
    public ResponseEntity<StrategyResponse> createStrategy(@Valid @RequestBody StrategyRequest request) {
        return ResponseEntity.ok(strategyService.create(request));
    }

    @GetMapping("/strategies")
    @Operation(summary = "전략 목록 조회", description = "등록된 전략 목록을 조회합니다.")
    public List<StrategyResponse> listStrategies(
            @Parameter(description = "심볼 필터 (예: AAPL)")
            @RequestParam(required = false) String symbol) {
        return strategyService.list(symbol);
    }

    @GetMapping("/strategies/{id}")
    @Operation(summary = "전략 상세 조회")
    public ResponseEntity<StrategyResponse> getStrategy(@PathVariable long id) {
        return strategyService.get(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping("/strategies/{id}")
    @Operation(summary = "전략 수정")
    public ResponseEntity<StrategyResponse> updateStrategy(
            @PathVariable long id,
            @Valid @RequestBody StrategyRequest request) {
        return strategyService.update(id, request)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/strategies/{id}")
    @Operation(summary = "전략 삭제")
    public ResponseEntity<Void> deleteStrategy(@PathVariable long id) {
        return strategyService.delete(id)
                ? ResponseEntity.noContent().build()
                : ResponseEntity.notFound().build();
    }

    // ----- 실행 -----

    @PostMapping("/strategies/{id}/run")
    @Operation(summary = "저장된 전략 백테스트 실행",
            description = "저장된 전략을 지정 기간의 일봉 데이터로 백테스트합니다.")
    public ResponseEntity<BacktestRunResponse> runStrategy(
            @PathVariable long id,
            @Parameter(description = "시작일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @Parameter(description = "종료일 (yyyy-MM-dd)")
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        return runService.runSavedStrategy(id, from, to)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/run")
    @Operation(summary = "즉석 백테스트 실행 (ad-hoc)",
            description = "전략을 저장하지 않고 요청 본문의 설정으로 백테스트를 실행합니다.")
    public ResponseEntity<BacktestRunResponse> runAdHoc(@Valid @RequestBody RunRequest request) {
        return ResponseEntity.ok(runService.runAdHoc(request));
    }

    @GetMapping("/strategies/{id}/runs")
    @Operation(summary = "전략별 실행 이력 조회")
    public List<BacktestRunResponse> listRunsByStrategy(@PathVariable long id) {
        return runService.getRunsByStrategy(id);
    }

    // ----- 결과 조회 / 리포트 -----

    @GetMapping("/runs/{runId}")
    @Operation(summary = "실행 결과 요약 조회",
            description = "누적 수익률, CAGR, MDD, 승률 등 요약 지표를 조회합니다.")
    public ResponseEntity<BacktestRunResponse> getRun(@PathVariable long runId) {
        return runService.getRun(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/runs/{runId}/trades")
    @Operation(summary = "체결 내역 조회", description = "백테스트 매수/매도 체결 내역을 조회합니다.")
    public ResponseEntity<List<TradeResponse>> getTrades(@PathVariable long runId) {
        return runService.getTrades(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/runs/{runId}/equity-curve")
    @Operation(summary = "자산 곡선 조회", description = "일자별 평가금액·낙폭(시각화용 데이터)을 조회합니다.")
    public ResponseEntity<List<EquityPointResponse>> getEquityCurve(@PathVariable long runId) {
        return runService.getEquityCurve(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
