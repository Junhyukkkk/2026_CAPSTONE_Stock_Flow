ALTER TABLE backtest_strategies
    DROP CONSTRAINT chk_backtest_strategy_type;

ALTER TABLE backtest_strategies
    ADD CONSTRAINT chk_backtest_strategy_type CHECK (
        strategy_type IN ('BUY_AND_HOLD', 'MA_CROSSOVER', 'RSI', 'PREDICTION')
    );
