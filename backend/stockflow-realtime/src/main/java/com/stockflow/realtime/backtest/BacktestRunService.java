package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.BacktestRunResponse;
import com.stockflow.realtime.backtest.dto.EquityPointResponse;
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
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.RunRow;
import com.stockflow.realtime.backtest.repository.BacktestRunRepository.TradeRow;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 백테스트 실행 오케스트레이션: 일봉 로딩 → 신호 생성 → 엔진 시뮬레이션 → 결과 저장.
 */
@Service
@RequiredArgsConstructor
public class BacktestRunService {

    private static final BigDecimal DEFAULT_INITIAL_CASH = BigDecimal.valueOf(10000);

    private final BacktestStrategyRepository strategyRepository;
    private final BacktestRunRepository runRepository;
    private final BacktestEngine engine;

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

    private BacktestRunResponse execute(Long strategyId, String symbol, StrategyType type,
                                        Map<String, Object> params, BigDecimal initialCash,
                                        LocalDate from, LocalDate to) {
        validateRange(from, to);
        if (initialCash == null || initialCash.signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }

        List<Bar> bars = runRepository.loadBars(symbol, from, to);
        if (bars.isEmpty()) {
            throw new NoDataException(
                    "No daily OHLCV data for symbol=" + symbol + " in range " + from + ".." + to);
        }

        try {
            TradingStrategy strategy = StrategyFactory.create(type, params);
            List<Signal> signals = strategy.generateSignals(bars);
            BacktestResult result = engine.run(bars, signals, initialCash);
            long runId = runRepository.saveResult(
                    strategyId, symbol, type.name(), params, from, to, result);
            return runRepository.findRun(runId).map(this::toRunResponse).orElseThrow();
        } catch (NoDataException e) {
            throw e;
        } catch (RuntimeException e) {
            // 예기치 못한 실패도 추적할 수 있도록 FAILED 로 기록 후 재던짐
            runRepository.saveFailure(strategyId, symbol, type.name(), params, from, to,
                    initialCash, e.getMessage());
            throw e;
        }
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

    /** 입력 데이터가 없을 때(404 매핑용). */
    public static class NoDataException extends RuntimeException {
        public NoDataException(String message) {
            super(message);
        }
    }
}
