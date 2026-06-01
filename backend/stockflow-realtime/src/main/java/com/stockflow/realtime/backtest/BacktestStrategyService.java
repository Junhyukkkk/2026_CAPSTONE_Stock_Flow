package com.stockflow.realtime.backtest;

import com.stockflow.realtime.backtest.dto.StrategyRequest;
import com.stockflow.realtime.backtest.dto.StrategyResponse;
import com.stockflow.realtime.backtest.engine.strategy.StrategyFactory;
import com.stockflow.realtime.backtest.model.StrategyType;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository;
import com.stockflow.realtime.backtest.repository.BacktestStrategyRepository.StrategyRow;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

/**
 * 백테스트 전략 CRUD. 저장 전에 전략 타입/파라미터 유효성을 검증한다.
 */
@Service
@RequiredArgsConstructor
public class BacktestStrategyService {

    private static final BigDecimal DEFAULT_INITIAL_CASH = BigDecimal.valueOf(10000);

    private final BacktestStrategyRepository repository;

    public StrategyResponse create(StrategyRequest req) {
        StrategyType type = validate(req);
        StrategyRow row = repository.insert(
                req.getName(), req.getSymbol(), type, req.getParams(), initialCash(req.getInitialCash()));
        return toResponse(row);
    }

    public List<StrategyResponse> list(String symbol) {
        return repository.findAll(symbol).stream().map(this::toResponse).toList();
    }

    public Optional<StrategyResponse> get(long id) {
        return repository.findById(id).map(this::toResponse);
    }

    public Optional<StrategyResponse> update(long id, StrategyRequest req) {
        StrategyType type = validate(req);
        return repository.update(
                        id, req.getName(), req.getSymbol(), type, req.getParams(), initialCash(req.getInitialCash()))
                .map(this::toResponse);
    }

    public boolean delete(long id) {
        return repository.delete(id);
    }

    /**
     * 전략 타입 파싱 + 파라미터 유효성 검사(StrategyFactory 생성으로 검증).
     *
     * @throws IllegalArgumentException 타입/파라미터가 유효하지 않은 경우
     */
    private StrategyType validate(StrategyRequest req) {
        StrategyType type = StrategyType.from(req.getStrategyType());
        if (req.getInitialCash() != null && req.getInitialCash().signum() <= 0) {
            throw new IllegalArgumentException("initialCash must be positive");
        }
        StrategyFactory.create(type, req.getParams()); // 파라미터 검증(잘못된 값이면 예외)
        return type;
    }

    private BigDecimal initialCash(BigDecimal value) {
        return value != null ? value : DEFAULT_INITIAL_CASH;
    }

    private StrategyResponse toResponse(StrategyRow row) {
        return StrategyResponse.builder()
                .id(row.id())
                .name(row.name())
                .symbol(row.symbol())
                .strategyType(row.strategyType())
                .params(row.params())
                .initialCash(row.initialCash())
                .createdAt(row.createdAt())
                .updatedAt(row.updatedAt())
                .build();
    }
}
