package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.EquityPointResponse;
import com.stockflow.realtime.backtest.dto.PredictionPointResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.RunRequest;
import com.stockflow.realtime.backtest.dto.TradeResponse;
import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.BacktestEngine;
import com.stockflow.realtime.backtest.engine.BacktestResult;
import com.stockflow.realtime.backtest.engine.Signal;
import com.stockflow.realtime.backtest.engine.strategy.StrategyFactory;
import com.stockflow.realtime.backtest.engine.strategy.TradingStrategy;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.EquityRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.PredictionPointRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.RunRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.TradeRow;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import com.stockflow.realtime.prediction.PredictionService;
import com.stockflow.realtime.prediction.PredictionSignalResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * 백테스트 실행 오케스트레이션: 일봉 로딩 → 신호 생성 → 엔진 시뮬레이션 → 결과 저장.
 */
@Service
@RequiredArgsConstructor
public class BacktestRunService {

    private static final BigDecimal DEFAULT_INITIAL_CASH = BigDecimal.valueOf(10000);
    private static final String DEFAULT_SOURCE = "BINANCE";
    private static final List<String> DEFAULT_REPORT_SYMBOLS = List.of(
            "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT",
            "ADAUSDT", "DOGEUSDT", "LTCUSDT", "LINKUSDT", "AVAXUSDT");
    private static final List<String> REPORT_MODELS = List.of(
            "ARIMA", "LOG_RETURN_ARIMA", "CHRONOS_BOLT");

    private final BacktestStrategyRepository strategyRepository;
    private final BacktestRunRepository runRepository;
    private final BacktestEngine engine;
    private final PredictionService predictionService;

    /** 저장된 전략으로 백테스트 실행. */
    @Transactional
    public Optional<BacktestRunResponse> runSavedStrategy(long strategyId, LocalDate from, LocalDate to) {
        Optional<StrategyRow> strategy = strategyRepository.findById(strategyId);
        if (strategy.isEmpty()) {
            return Optional.empty();
        }
        StrategyRow s = strategy.get();
        return Optional.of(execute(
                strategyId, s.symbol(), StrategyType.from(s.strategyType()),
                s.params(), s.initialCash(), from, to));
    }

    /** 저장하지 않고 즉석 실행(ad-hoc). */
    @Transactional
    public BacktestRunResponse runAdHoc(RunRequest req) {
        StrategyType type = StrategyType.from(req.getStrategyType());
        BigDecimal initialCash = req.getInitialCash() != null ? req.getInitialCash() : DEFAULT_INITIAL_CASH;
        return execute(null, req.getSymbol(), type, req.getParams(), initialCash, req.getFrom(), req.getTo());
    }

    /** 동일 조건으로 대표 암호화폐와 예측 모델의 성과를 일괄 집계한다. */
    @Transactional
    public PerformanceReportResponse generatePerformanceReport(PerformanceReportRequest request) {
        validateRange(request.getFrom(), request.getTo());
        BigDecimal initialCash = request.getInitialCash() != null
                ? request.getInitialCash() : DEFAULT_INITIAL_CASH;
        if (initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }

        List<PerformanceReportRow> rows = new ArrayList<>();
        for (String symbol : normalizeReportSymbols(request.getSymbols())) {
            rows.add(runReportRow(symbol, StrategyType.BUY_AND_HOLD, null, Map.of(), initialCash,
                    request.getFrom(), request.getTo()));
            for (String model : REPORT_MODELS) {
                rows.add(runReportRow(symbol, StrategyType.PREDICTION, model,
                        reportPredictionParams(model), initialCash, request.getFrom(), request.getTo()));
            }
        }
        return new PerformanceReportResponse(
                request.getFrom(), request.getTo(), initialCash, rows);
    }

    private BacktestRunResponse execute(Long strategyId, String symbol, StrategyType type,
                                        Map<String, Object> params, BigDecimal initialCash,
                                        LocalDate from, LocalDate to) {
        validateRange(from, to);
        if (initialCash == null || initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }

        Map<String, Object> effectiveParams = params == null ? Map.of() : params;
        try {
            PredictionBacktestConfig predictionConfig = type == StrategyType.PREDICTION
                    ? PredictionBacktestConfig.from(effectiveParams)
                    : null;
            String source = predictionConfig == null ? DEFAULT_SOURCE : predictionConfig.source();
            List<Bar> bars = runRepository.loadBars(symbol, source, from, to);
            if (bars.isEmpty()) {
                throw new NoDataException(
                        "No daily OHLCV data for symbol=" + symbol + " in range " + from + ".." + to);
            }
            List<Signal> signals;
            BacktestResult result;
            List<PredictionSignalResponse.PredictionSignalPoint> predictionPoints = List.of();
            if (type == StrategyType.PREDICTION) {
                PredictionBacktestConfig config = predictionConfig;
                effectiveParams = config.asParams();
                int historyCount = runRepository.countBarsBefore(symbol, config.source(), from);
                if (historyCount < config.warmup()) {
                    throw new IllegalArgumentException(
                            "at least " + config.warmup()
                                    + " observations are required before from_date"
                                    + " (found " + historyCount + " actual daily observations)");
                }
                PredictionSignalResponse response = predictionService.backtestSignals(
                        config.toRequest(symbol, from, to));
                predictionPoints = response.signals();
                Map<String, Object> predictionParams = new LinkedHashMap<>(config.asParams());
                predictionParams.put("buySignalCount", response.buyCount());
                predictionParams.put("holdSignalCount", response.holdCount());
                predictionParams.put("sellSignalCount", response.sellCount());
                predictionParams.put("mae", response.mae());
                predictionParams.put("rmse", response.rmse());
                predictionParams.put("maePct", response.maePct());
                predictionParams.put("rmsePct", response.rmsePct());
                effectiveParams = predictionParams;
                signals = alignPredictionSignals(bars, response);
                result = engine.run(bars, signals, initialCash, config.executionConfig());
            } else {
                TradingStrategy strategy = StrategyFactory.create(type, effectiveParams);
                signals = strategy.generateSignals(bars);
                result = engine.run(bars, signals, initialCash);
            }
            long runId = runRepository.saveResult(
                    strategyId, symbol, type.name(), effectiveParams, from, to, result);
            runRepository.savePredictionPoints(runId, predictionPoints);
            return runRepository.findRun(runId).map(this::toRunResponse).orElseThrow();
        } catch (NoDataException e) {
            throw e;
        } catch (RuntimeException e) {
            // 예기치 못한 실패도 추적할 수 있도록 FAILED 로 기록 후 재던짐
            runRepository.saveFailure(strategyId, symbol, type.name(), effectiveParams, from, to,
                    initialCash, e.getMessage());
            throw e;
        }
    }

    private List<Signal> alignPredictionSignals(
            List<Bar> bars, PredictionSignalResponse response) {
        if (response.signals() == null) {
            throw new IllegalStateException("Prediction service returned no signals");
        }
        Map<LocalDate, Signal> byExecutionDate = new HashMap<>();
        for (PredictionSignalResponse.PredictionSignalPoint point : response.signals()) {
            Signal previous = byExecutionDate.put(
                    point.executionDate(), Signal.valueOf(point.signal().toUpperCase()));
            if (previous != null) {
                throw new IllegalStateException(
                        "Duplicate prediction signal date: " + point.executionDate());
            }
        }
        return bars.stream()
                .map(bar -> {
                    Signal signal = byExecutionDate.get(bar.date());
                    if (signal == null) {
                        throw new IllegalStateException(
                                "Missing prediction signal for execution date: " + bar.date());
                    }
                    return signal;
                })
                .toList();
    }

    private PerformanceReportRow runReportRow(
            String symbol, StrategyType type, String model, Map<String, Object> params,
            BigDecimal initialCash, LocalDate from, LocalDate to) {
        try {
            BacktestRunResponse run = execute(null, symbol, type, params, initialCash, from, to);
            Map<String, Object> resultParams = run.getParams();
            return new PerformanceReportRow(
                    symbol, type.name(), model, "SUCCESS", run.getId(), run.getTotalReturnPct(),
                    run.getMddPct(), decimalParam(resultParams, "mae"), decimalParam(resultParams, "rmse"),
                    decimalParam(resultParams, "maePct"), decimalParam(resultParams, "rmsePct"),
                    intParam(resultParams, "buySignalCount"), intParam(resultParams, "holdSignalCount"),
                    intParam(resultParams, "sellSignalCount"), run.getTradeCount(), null);
        } catch (RuntimeException e) {
            return new PerformanceReportRow(
                    symbol, type.name(), model, "FAILED",
                    null, null, null, null, null, null, null,
                    null, null, null, null, e.getMessage());
        }
    }

    private List<String> normalizeReportSymbols(List<String> symbols) {
        List<String> source = symbols == null || symbols.isEmpty() ? DEFAULT_REPORT_SYMBOLS : symbols;
        List<String> normalized = source.stream()
                .filter(symbol -> symbol != null && !symbol.isBlank())
                .map(symbol -> symbol.trim().toUpperCase(Locale.ROOT))
                .distinct()
                .toList();
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("at least one symbol is required");
        }
        return normalized;
    }

    private Map<String, Object> reportPredictionParams(String model) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("model", model);
        params.put("source", DEFAULT_SOURCE);
        params.put("warmup", 50);
        params.put("refitEvery", 5);
        params.put("maxHistory", 200);
        params.put("volatilityWindow", 20);
        params.put("volatilityMultiplier", 0.25);
        params.put("feeBps", 10.0);
        params.put("slippageBps", 5.0);
        return params;
    }

    private BigDecimal decimalParam(Map<String, Object> params, String key) {
        Object value = params.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal;
        }
        return new BigDecimal(value.toString());
    }

    private Integer intParam(Map<String, Object> params, String key) {
        Object value = params.get(key);
        return value instanceof Number number ? number.intValue() : null;
    }

    public Optional<BacktestRunResponse> getRun(long runId) {
        return runRepository.findRun(runId).map(this::toRunResponse);
    }

    public List<BacktestRunResponse> getRunsByStrategy(long strategyId) {
        return runRepository.findRunsByStrategy(strategyId).stream().map(this::toRunResponse).toList();
    }

    public Optional<List<TradeResponse>> getTrades(long runId) {
        if (!runRepository.runExists(runId)) {
            return Optional.empty();
        }
        return Optional.of(runRepository.findTrades(runId).stream().map(this::toTradeResponse).toList());
    }

    public Optional<List<EquityPointResponse>> getEquityCurve(long runId) {
        if (!runRepository.runExists(runId)) {
            return Optional.empty();
        }
        return Optional.of(runRepository.findEquityCurve(runId).stream().map(this::toEquityResponse).toList());
    }

    public Optional<List<PredictionPointResponse>> getPredictionPoints(long runId) {
        if (!runRepository.runExists(runId)) {
            return Optional.empty();
        }
        return Optional.of(runRepository.findPredictionPoints(runId).stream()
                .map(this::toPredictionPointResponse)
                .toList());
    }

    private void validateRange(LocalDate from, LocalDate to) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("from and to dates are required");
        }
        if (from.isAfter(to)) {
            throw new IllegalArgumentException("from must be <= to");
        }
    }

    private BacktestRunResponse toRunResponse(RunRow r) {
        return BacktestRunResponse.builder()
                .id(r.id())
                .strategyId(r.strategyId())
                .symbol(r.symbol())
                .strategyType(r.strategyType())
                .params(r.params())
                .fromDate(r.fromDate())
                .toDate(r.toDate())
                .initialCash(r.initialCash())
                .finalEquity(r.finalEquity())
                .totalReturnPct(r.totalReturnPct())
                .cagrPct(r.cagrPct())
                .mddPct(r.mddPct())
                .tradeCount(r.tradeCount())
                .winRatePct(r.winRatePct())
                .barCount(r.barCount())
                .status(r.status())
                .createdAt(r.createdAt())
                .build();
    }

    private TradeResponse toTradeResponse(TradeRow t) {
        return TradeResponse.builder()
                .seq(t.seq())
                .tradeDate(t.tradeDate())
                .side(t.side())
                .price(t.price())
                .quantity(t.quantity())
                .cashAfter(t.cashAfter())
                .equityAfter(t.equityAfter())
                .pnlPct(t.pnlPct())
                .build();
    }

    private EquityPointResponse toEquityResponse(EquityRow e) {
        return EquityPointResponse.builder()
                .tradeDate(e.tradeDate())
                .equity(e.equity())
                .drawdownPct(e.drawdownPct())
                .build();
    }

    private PredictionPointResponse toPredictionPointResponse(PredictionPointRow point) {
        return PredictionPointResponse.builder()
                .signalDate(point.signalDate())
                .executionDate(point.executionDate())
                .referencePrice(point.referencePrice())
                .predictedPrice(point.predictedPrice())
                .expectedReturnPct(point.expectedReturnPct())
                .thresholdPct(point.thresholdPct())
                .signal(point.signal())
                .build();
    }

    /** 입력 데이터가 없을 때(404 매핑용). */
    public static class NoDataException extends RuntimeException {
        public NoDataException(String message) {
            super(message);
        }
    }
}
