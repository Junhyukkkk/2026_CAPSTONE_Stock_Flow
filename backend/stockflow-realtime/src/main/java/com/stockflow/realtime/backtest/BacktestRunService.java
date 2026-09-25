package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.BacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestDataReadinessResponse;
import com.stockflow.realtime.backtest.dto.IntradayBacktestResponse;
import com.stockflow.realtime.backtest.dto.IntradayRunRequest;
import com.stockflow.realtime.backtest.dto.EquityPointResponse;
import com.stockflow.realtime.backtest.dto.PredictionPointResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportRequest;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportUniverseResponse;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportRow;
import com.stockflow.realtime.backtest.dto.PerformanceReportResponse.PerformanceReportSummary;
import com.stockflow.realtime.backtest.dto.ThresholdReportRequest;
import com.stockflow.realtime.backtest.dto.ThresholdReportResponse;
import com.stockflow.realtime.backtest.dto.ThresholdReportResponse.ThresholdSummary;
import com.stockflow.realtime.backtest.dto.RunRequest;
import com.stockflow.realtime.backtest.dto.TradeResponse;
import com.stockflow.realtime.backtest.engine.Bar;
import com.stockflow.realtime.backtest.engine.BacktestEngine;
import com.stockflow.realtime.backtest.engine.BacktestResult;
import com.stockflow.realtime.backtest.engine.Signal;
import com.stockflow.realtime.backtest.engine.strategy.StrategyFactory;
import com.stockflow.realtime.backtest.engine.strategy.TradingStrategy;
import com.stockflow.realtime.backtest.intraday.IntradayBacktestEngine;
import com.stockflow.realtime.backtest.intraday.IntradayBacktestResult;
import com.stockflow.realtime.backtest.intraday.IntradayBar;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.EquityRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.DataCoverage;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.PredictionPointRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.RunRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.TradeRow;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import com.stockflow.realtime.prediction.PredictionService;
import com.stockflow.realtime.prediction.PredictionSignalResponse;
import com.stockflow.realtime.prediction.IntradayPredictionSignalResponse;
import com.stockflow.realtime.stock.IntradayOhlcvService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.Instant;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

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
    private static final BigDecimal DEFAULT_VOLATILITY_MULTIPLIER = BigDecimal.valueOf(0.25);
    private static final List<BigDecimal> THRESHOLD_COMPARISON_MULTIPLIERS = List.of(
            BigDecimal.valueOf(0.15), DEFAULT_VOLATILITY_MULTIPLIER, BigDecimal.valueOf(0.35));

    private final BacktestStrategyRepository strategyRepository;
    private final BacktestRunRepository runRepository;
    private final BacktestEngine engine;
    private final PredictionService predictionService;
    private final IntradayOhlcvService intradayOhlcvService;
    private final IntradayBacktestEngine intradayBacktestEngine;

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

    /**
     * 실행 전 화면에서 사용하는 일봉 데이터 준비 상태 조회.
     * 실행 로직과 같은 source·실제 관측치 기준으로 계산하지만, 이 메서드는 데이터를 변경하지 않는다.
     */
    public BacktestDataReadinessResponse inspectDataReadiness(
            String symbol, LocalDate from, LocalDate to, String source, int minimumHistoryDays) {
        validateRange(from, to);
        if (minimumHistoryDays < 0) {
            throw new IllegalArgumentException("minimumHistoryDays must be zero or greater");
        }

        String normalizedSymbol = symbol == null ? "" : symbol.trim().toUpperCase(Locale.ROOT);
        if (normalizedSymbol.isEmpty()) {
            throw new IllegalArgumentException("symbol is required");
        }
        String resolvedSource = source == null || source.isBlank()
                ? DEFAULT_SOURCE : source.trim().toUpperCase(Locale.ROOT);
        List<Bar> selectedBars = runRepository.loadBars(normalizedSymbol, resolvedSource, from, to);
        int historyBarCount = runRepository.countBarsBefore(normalizedSymbol, resolvedSource, from);
        DataCoverage coverage = runRepository.findCoverage(normalizedSymbol, resolvedSource);
        int expectedBarCount = Math.toIntExact(ChronoUnit.DAYS.between(from, to) + 1);
        int missingBarCount = Math.max(0, expectedBarCount - selectedBars.size());

        boolean hasSelectedBars = !selectedBars.isEmpty();
        boolean hasRequiredHistory = minimumHistoryDays == 0 || historyBarCount >= minimumHistoryDays;
        boolean hasCompleteSelectedRange = missingBarCount == 0;
        boolean canRun = hasSelectedBars && hasRequiredHistory && hasCompleteSelectedRange;
        String status;
        String message;
        if (!hasSelectedBars) {
            status = "BLOCKED";
            message = "선택한 기간에 사용할 수 있는 일봉 데이터가 없습니다.";
        } else if (!hasRequiredHistory) {
            status = "BLOCKED";
            message = "시작일 이전의 실제 학습 데이터가 부족합니다.";
        } else if (!hasCompleteSelectedRange) {
            status = "BLOCKED";
            message = "선택한 구간에 누락된 일봉이 있어 예측 백테스트를 실행할 수 없습니다.";
        } else {
            status = "READY";
            message = "선택한 조건으로 백테스트를 실행할 수 있습니다.";
        }

        return new BacktestDataReadinessResponse(
                normalizedSymbol, resolvedSource, from, to,
                coverage.firstDate(), coverage.lastDate(),
                selectedBars.size(), expectedBarCount, missingBarCount,
                historyBarCount, minimumHistoryDays, canRun, status, message);
    }

    /**
     * 분봉 백테스트 확장의 1단계. 선택 구간과 직전 학습 구간만 제한 조회해 데이터 연속성을 확인한다.
     * 현재 예측 API가 지원하는 분봉은 1m뿐이므로 5m는 별도 모델 확장 전까지 명시적으로 막는다.
     */
    public IntradayBacktestDataReadinessResponse inspectIntradayDataReadiness(
            String symbol, Instant from, Instant to, String interval, String source, int minimumHistoryBars) {
        if (!"1m".equals(interval)) {
            throw new IllegalArgumentException("intraday backtest readiness currently supports only 1m");
        }
        if (from == null || to == null || !from.isBefore(to)) {
            throw new IllegalArgumentException("from must be earlier than to");
        }
        if (Duration.between(from, to).compareTo(Duration.ofHours(6)) > 0) {
            throw new IllegalArgumentException("intraday readiness range must be within 6 hours");
        }
        if (minimumHistoryBars < 50 || minimumHistoryBars > 500) {
            throw new IllegalArgumentException("minimumHistoryBars must be between 50 and 500");
        }

        Instant start = from.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        Instant end = to.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("at least one full one-minute bar is required");
        }
        String normalizedSymbol = symbol == null ? "" : symbol.trim().toUpperCase(Locale.ROOT);
        if (normalizedSymbol.isEmpty()) {
            throw new IllegalArgumentException("symbol is required");
        }
        String resolvedSource = source == null || source.isBlank()
                ? DEFAULT_SOURCE : source.trim().toUpperCase(Locale.ROOT);
        int expected = Math.toIntExact(Duration.between(start, end).toMinutes());
        Instant historyFrom = start.minus(Duration.ofMinutes((long) minimumHistoryBars * 2));
        List<com.stockflow.realtime.stock.dto.IntradayOhlcvResponse> selectedBars =
                intradayOhlcvService.getIntraday(normalizedSymbol, interval, start, end);
        List<com.stockflow.realtime.stock.dto.IntradayOhlcvResponse> historyBars =
                intradayOhlcvService.getIntraday(normalizedSymbol, interval, historyFrom, start);
        int selected = selectedBars.size();
        int history = historyBars.size();
        int missing = Math.max(0, expected - selected);
        boolean canUse = selected > 0 && missing == 0 && history >= minimumHistoryBars;
        String status = canUse ? "READY" : "BLOCKED";
        String message = selected == 0
                ? "선택한 구간에 사용할 수 있는 1분봉 데이터가 없습니다."
                : missing > 0
                ? "선택한 구간에 누락된 1분봉이 있어 분봉 백테스트를 실행할 수 없습니다."
                : history < minimumHistoryBars
                ? "시작 시각 이전의 실제 1분봉 학습 데이터가 부족합니다."
                : "1분봉 데이터가 연속적으로 준비되어 다음 단계의 백테스트 엔진 연결이 가능합니다.";
        return new IntradayBacktestDataReadinessResponse(
                normalizedSymbol, resolvedSource, interval, start, end,
                historyBars.isEmpty() ? (selectedBars.isEmpty() ? null : selectedBars.get(0).getTime())
                        : historyBars.get(0).getTime(),
                selectedBars.isEmpty() ? (historyBars.isEmpty() ? null : historyBars.get(historyBars.size() - 1).getTime())
                        : selectedBars.get(selectedBars.size() - 1).getTime(),
                selected, expected, missing,
                history, minimumHistoryBars, canUse, status, message);
    }

    /**
     * BTCUSDT 1분봉 전략을 즉석 실행한다. 일봉 실행·저장 테이블과 분리해 결과를 반환한다.
     */
    public IntradayBacktestResponse runIntraday(IntradayRunRequest request) {
        String symbol = normalizeIntradaySymbol(request.getSymbol());
        StrategyType strategyType = StrategyType.from(request.getStrategyType());
        if (strategyType != StrategyType.BUY_AND_HOLD
                && strategyType != StrategyType.MA_CROSSOVER
                && strategyType != StrategyType.PREDICTION) {
            throw new IllegalArgumentException(
                    "intraday backtest currently supports BUY_AND_HOLD, MA_CROSSOVER, and PREDICTION");
        }
        Instant start = truncateToMinute(request.getFrom());
        Instant end = truncateToMinute(request.getTo());
        validateIntradayRange(start, end);

        BigDecimal initialCash = request.getInitialCash() == null ? DEFAULT_INITIAL_CASH : request.getInitialCash();
        if (initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }
        List<IntradayBar> selectedBars = toIntradayBars(
                intradayOhlcvService.getIntraday(symbol, "1m", start, end));
        int expectedBars = Math.toIntExact(Duration.between(start, end).toMinutes());
        if (selectedBars.size() != expectedBars) {
            throw new NoDataException("선택한 구간의 1분봉이 연속적이지 않습니다. 누락 구간을 채운 뒤 다시 실행해주세요.");
        }

        Map<String, Object> params = request.getParams() == null ? Map.of() : new LinkedHashMap<>(request.getParams());
        if (request.getFeeBps() != null) {
            params.put("feeBps", request.getFeeBps());
        }
        if (request.getSlippageBps() != null) {
            params.put("slippageBps", request.getSlippageBps());
        }
        BigDecimal feeBps;
        BigDecimal slippageBps;
        List<Signal> selectedSignals;
        Signal initialSignal;
        if (strategyType == StrategyType.BUY_AND_HOLD) {
            feeBps = decimalParamOrDefault(params, "feeBps", BigDecimal.TEN);
            slippageBps = decimalParamOrDefault(params, "slippageBps", BigDecimal.valueOf(5));
            validateBasisPoints(feeBps, "feeBps");
            validateBasisPoints(slippageBps, "slippageBps");
            selectedSignals = holdSignals(selectedBars.size());
            initialSignal = Signal.BUY;
        } else if (strategyType == StrategyType.MA_CROSSOVER) {
            feeBps = decimalParamOrDefault(params, "feeBps", BigDecimal.TEN);
            slippageBps = decimalParamOrDefault(params, "slippageBps", BigDecimal.valueOf(5));
            validateBasisPoints(feeBps, "feeBps");
            validateBasisPoints(slippageBps, "slippageBps");
            int shortPeriod = positiveIntParam(params, "shortPeriod", 5);
            int longPeriod = positiveIntParam(params, "longPeriod", 20);
            if (shortPeriod >= longPeriod || longPeriod > 240) {
                throw new IllegalArgumentException("MA periods must satisfy 1 <= shortPeriod < longPeriod <= 240");
            }
            params.put("shortPeriod", shortPeriod);
            params.put("longPeriod", longPeriod);
            int requiredHistory = longPeriod + 1;
            Instant historyFrom = start.minus(Duration.ofMinutes(requiredHistory));
            List<IntradayBar> historyBars = toIntradayBars(
                    intradayOhlcvService.getIntraday(symbol, "1m", historyFrom, start));
            if (historyBars.size() != requiredHistory) {
                throw new NoDataException("MA Crossover에는 시작 시각 이전의 연속된 1분봉 "
                        + requiredHistory + "개가 필요합니다.");
            }
            List<IntradayBar> signalBars = new ArrayList<>(historyBars.size() + selectedBars.size());
            signalBars.addAll(historyBars);
            signalBars.addAll(selectedBars);
            List<Signal> allSignals = maCrossoverSignals(signalBars, shortPeriod, longPeriod);
            initialSignal = allSignals.get(historyBars.size() - 1);
            selectedSignals = new ArrayList<>(allSignals.subList(historyBars.size(), allSignals.size()));
        } else {
            IntradayPredictionBacktestConfig predictionConfig = IntradayPredictionBacktestConfig.from(params);
            if ("CHRONOS_BOLT".equals(predictionConfig.model()) && selectedBars.size() > 60) {
                throw new IllegalArgumentException(
                        "Chronos-Bolt 1분봉 백테스트는 CPU 응답 시간을 고려해 현재 최대 60분까지만 지원합니다.");
            }
            IntradayPredictionSignalResponse response = predictionService.backtestIntradaySignals(
                    predictionConfig.toRequest(symbol, start, end));
            IntradaySignals predictionSignals = alignIntradayPredictionSignals(selectedBars, response);
            initialSignal = predictionSignals.initialSignal();
            selectedSignals = predictionSignals.signals();
            feeBps = BigDecimal.valueOf(predictionConfig.feeBps());
            slippageBps = BigDecimal.valueOf(predictionConfig.slippageBps());
            params = new LinkedHashMap<>(predictionConfig.asParams());
            params.put("buySignalCount", response.buyCount());
            params.put("holdSignalCount", response.holdCount());
            params.put("sellSignalCount", response.sellCount());
            params.put("mae", response.mae());
            params.put("rmse", response.rmse());
            params.put("maePct", response.maePct());
            params.put("rmsePct", response.rmsePct());
        }

        IntradayBacktestResult result = intradayBacktestEngine.run(
                selectedBars, selectedSignals, initialSignal, initialCash,
                IntradayBacktestEngine.ExecutionConfig.fromBasisPoints(feeBps, slippageBps));
        return new IntradayBacktestResponse(
                symbol, DEFAULT_SOURCE, "1m", strategyType.name(), Map.copyOf(params), start, end,
                result.initialCash(), feeBps, slippageBps, result.finalEquity(), result.totalReturnPct(),
                result.mddPct(), result.roundTripCount(), result.winRatePct(), result.barCount(),
                result.trades().stream().map(IntradayBacktestResponse.Trade::from).toList(),
                result.equityCurve().stream().map(IntradayBacktestResponse.EquityPoint::from).toList());
    }

    private String normalizeIntradaySymbol(String symbol) {
        String normalized = symbol == null ? "" : symbol.trim().toUpperCase(Locale.ROOT);
        if (!"BTCUSDT".equals(normalized)) {
            throw new IllegalArgumentException("intraday backtest currently supports BTCUSDT only");
        }
        return normalized;
    }

    private static Instant truncateToMinute(Instant time) {
        if (time == null) {
            throw new IllegalArgumentException("from and to times are required");
        }
        return time.truncatedTo(ChronoUnit.MINUTES);
    }

    private static void validateIntradayRange(Instant from, Instant to) {
        if (!from.isBefore(to)) {
            throw new IllegalArgumentException("from must be earlier than to");
        }
        if (Duration.between(from, to).compareTo(Duration.ofHours(6)) > 0) {
            throw new IllegalArgumentException("intraday backtest range must be within 6 hours");
        }
    }

    private static void validateBasisPoints(BigDecimal value, String name) {
        if (value.signum() < 0 || value.compareTo(BigDecimal.valueOf(1000)) > 0) {
            throw new IllegalArgumentException(name + " must be between 0 and 1000");
        }
    }

    private static int positiveIntParam(Map<String, Object> params, String key, int defaultValue) {
        Object raw = params.get(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            int value = raw instanceof Number number ? number.intValue() : Integer.parseInt(raw.toString());
            if (value < 1) {
                throw new IllegalArgumentException(key + " must be at least 1");
            }
            return value;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(key + " must be an integer");
        }
    }

    private static BigDecimal decimalParamOrDefault(Map<String, Object> params, String key, BigDecimal defaultValue) {
        Object raw = params.get(key);
        if (raw == null) {
            return defaultValue;
        }
        try {
            return raw instanceof BigDecimal value ? value : new BigDecimal(raw.toString());
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(key + " must be a number", exception);
        }
    }

    /**
     * FastAPI는 signal_time의 종가를 보고 execution_time(다음 1분)의 신호를 반환한다.
     * 엔진은 signals[i - 1]을 i번째 분의 시가에 체결하므로 실행 시각 기준으로 한 칸 맞춘다.
     */
    private IntradaySignals alignIntradayPredictionSignals(
            List<IntradayBar> bars, IntradayPredictionSignalResponse response) {
        if (response.signals() == null) {
            throw new IllegalStateException("Prediction service returned no intraday signals");
        }
        Map<Instant, Signal> byExecutionTime = new HashMap<>();
        for (IntradayPredictionSignalResponse.IntradayPredictionSignalPoint point : response.signals()) {
            Instant executionTime = truncateToMinute(point.executionTime());
            Signal signal;
            try {
                signal = Signal.valueOf(point.signal().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException exception) {
                throw new IllegalStateException("Invalid intraday prediction signal: " + point.signal(), exception);
            }
            if (byExecutionTime.put(executionTime, signal) != null) {
                throw new IllegalStateException("Duplicate prediction signal time: " + executionTime);
            }
        }

        List<Signal> signalsAtExecutionTime = bars.stream()
                .map(bar -> {
                    Signal signal = byExecutionTime.get(truncateToMinute(bar.time()));
                    if (signal == null) {
                        throw new IllegalStateException(
                                "Missing prediction signal for execution time: " + bar.time());
                    }
                    return signal;
                })
                .toList();
        List<Signal> delayedSignals = new ArrayList<>(bars.size());
        for (int index = 0; index < bars.size(); index++) {
            delayedSignals.add(index + 1 < bars.size()
                    ? signalsAtExecutionTime.get(index + 1) : Signal.HOLD);
        }
        return new IntradaySignals(signalsAtExecutionTime.get(0), delayedSignals);
    }

    private record IntradaySignals(Signal initialSignal, List<Signal> signals) {
    }

    private static List<IntradayBar> toIntradayBars(
            List<com.stockflow.realtime.stock.dto.IntradayOhlcvResponse> candles) {
        return candles.stream()
                .map(candle -> new IntradayBar(candle.getTime(), candle.getOpen(), candle.getHigh(), candle.getLow(),
                        candle.getClose(), candle.getVolume()))
                .toList();
    }

    private static List<Signal> holdSignals(int size) {
        return java.util.Collections.nCopies(size, Signal.HOLD);
    }

    private static List<Signal> maCrossoverSignals(List<IntradayBar> bars, int shortPeriod, int longPeriod) {
        List<Signal> signals = new ArrayList<>(bars.size());
        double[] closes = new double[bars.size()];
        for (int i = 0; i < bars.size(); i++) {
            closes[i] = bars.get(i).close().doubleValue();
            if (i < longPeriod) {
                signals.add(Signal.HOLD);
                continue;
            }
            double shortNow = sma(closes, i, shortPeriod);
            double longNow = sma(closes, i, longPeriod);
            double shortPrevious = sma(closes, i - 1, shortPeriod);
            double longPrevious = sma(closes, i - 1, longPeriod);
            signals.add(shortPrevious <= longPrevious && shortNow > longNow ? Signal.BUY
                    : shortPrevious >= longPrevious && shortNow < longNow ? Signal.SELL : Signal.HOLD);
        }
        return signals;
    }

    private static double sma(double[] values, int endIndex, int period) {
        double total = 0;
        for (int i = endIndex - period + 1; i <= endIndex; i++) {
            total += values[i];
        }
        return total / period;
    }

    /**
     * 전체 성과 리포트 실행 전에 동일한 데이터 연속성·학습 조건을 충족하는 코인만 선별한다.
     * 실제 모델 실행은 별도 리포트 작업에서 수행한다.
     */
    public PerformanceReportUniverseResponse inspectPerformanceReportUniverse(
            LocalDate from, LocalDate to, int minimumHistoryDays) {
        validateRange(from, to);
        if (minimumHistoryDays < 50 || minimumHistoryDays > 500) {
            throw new IllegalArgumentException("minimumHistoryDays must be between 50 and 500");
        }
        List<String> eligibleSymbols = runRepository.findEligibleCryptoSymbols(
                DEFAULT_SOURCE, from, to, minimumHistoryDays);
        return new PerformanceReportUniverseResponse(
                from, to, minimumHistoryDays,
                runRepository.countKnownCryptoSymbols(DEFAULT_SOURCE),
                eligibleSymbols.size(), eligibleSymbols);
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
                request.getFrom(), request.getTo(), initialCash, rows, summarizeReportRows(rows));
    }

    /** 동일 모델에서 신호 기준 계수만 바꿔 대표 암호화폐 성과를 비교한다. */
    @Transactional
    public ThresholdReportResponse generateThresholdReport(ThresholdReportRequest request) {
        validateRange(request.getFrom(), request.getTo());
        BigDecimal initialCash = request.getInitialCash() != null
                ? request.getInitialCash() : DEFAULT_INITIAL_CASH;
        if (initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }
        String model = request.getModel().trim().toUpperCase(Locale.ROOT);
        if (!REPORT_MODELS.contains(model)) {
            throw new IllegalArgumentException("unsupported prediction model: " + request.getModel());
        }

        List<ThresholdSummary> summaries = new ArrayList<>();
        for (BigDecimal multiplier : THRESHOLD_COMPARISON_MULTIPLIERS) {
            List<PerformanceReportRow> rows = new ArrayList<>();
            for (String symbol : normalizeReportSymbols(request.getSymbols())) {
                rows.add(runReportRow(symbol, StrategyType.PREDICTION, model,
                        reportPredictionParams(model, multiplier), initialCash,
                        request.getFrom(), request.getTo()));
            }
            PerformanceReportSummary summary = summarizeReportRows(rows, StrategyType.PREDICTION, model);
            summaries.add(new ThresholdSummary(
                    multiplier, summary.successfulRuns(), summary.failedRuns(), summary.positiveReturnCount(),
                    summary.averageTotalReturnPct(), summary.averageMddPct(), summary.averageMaePct(),
                    summary.averageRmsePct(), summary.averageTradeCount()));
        }
        return new ThresholdReportResponse(
                request.getFrom(), request.getTo(), initialCash, model, summaries);
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

    @Transactional
    public PerformanceReportRow runPerformanceReportRow(
            String symbol, StrategyType type, String model, Map<String, Object> params,
            BigDecimal initialCash, LocalDate from, LocalDate to) {
        return runReportRow(symbol, type, model, params, initialCash, from, to);
    }

    public List<PerformanceReportSummary> summarizePerformanceReportRows(List<PerformanceReportRow> rows) {
        return summarizeReportRows(rows);
    }

    public Map<String, Object> defaultReportPredictionParams(String model) {
        return reportPredictionParams(model);
    }

    public List<String> reportModels() {
        return REPORT_MODELS;
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

    private List<PerformanceReportSummary> summarizeReportRows(List<PerformanceReportRow> rows) {
        List<PerformanceReportSummary> summaries = new ArrayList<>();
        summaries.add(summarizeReportRows(rows, StrategyType.BUY_AND_HOLD, null));
        for (String model : REPORT_MODELS) {
            summaries.add(summarizeReportRows(rows, StrategyType.PREDICTION, model));
        }
        return summaries;
    }

    private PerformanceReportSummary summarizeReportRows(
            List<PerformanceReportRow> rows, StrategyType type, String model) {
        List<PerformanceReportRow> matching = rows.stream()
                .filter(row -> type.name().equals(row.strategyType()))
                .filter(row -> model == null ? row.model() == null : model.equals(row.model()))
                .toList();
        List<PerformanceReportRow> successful = matching.stream()
                .filter(row -> "SUCCESS".equals(row.status()))
                .toList();
        int positiveReturnCount = (int) successful.stream()
                .filter(row -> row.totalReturnPct() != null && row.totalReturnPct().signum() > 0)
                .count();
        return new PerformanceReportSummary(
                type.name(), model,
                successful.size(), matching.size() - successful.size(), positiveReturnCount,
                average(successful, PerformanceReportRow::totalReturnPct),
                average(successful, PerformanceReportRow::mddPct),
                average(successful, PerformanceReportRow::maePct),
                average(successful, PerformanceReportRow::rmsePct),
                average(successful, row -> row.tradeCount() == null
                        ? null : BigDecimal.valueOf(row.tradeCount())));
    }

    private BigDecimal average(
            List<PerformanceReportRow> rows, Function<PerformanceReportRow, BigDecimal> valueExtractor) {
        List<BigDecimal> values = rows.stream()
                .map(valueExtractor)
                .filter(value -> value != null)
                .toList();
        if (values.isEmpty()) {
            return null;
        }
        BigDecimal total = values.stream().reduce(BigDecimal.ZERO, BigDecimal::add);
        return total.divide(BigDecimal.valueOf(values.size()), 6, RoundingMode.HALF_UP);
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
        return reportPredictionParams(model, DEFAULT_VOLATILITY_MULTIPLIER);
    }

    private Map<String, Object> reportPredictionParams(String model, BigDecimal volatilityMultiplier) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("model", model);
        params.put("source", DEFAULT_SOURCE);
        params.put("warmup", 50);
        params.put("refitEvery", 5);
        params.put("maxHistory", 200);
        params.put("volatilityWindow", 20);
        params.put("volatilityMultiplier", volatilityMultiplier.doubleValue());
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
