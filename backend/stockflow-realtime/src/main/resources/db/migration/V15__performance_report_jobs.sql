CREATE TABLE IF NOT EXISTS backtest_performance_report_jobs (
    id BIGSERIAL PRIMARY KEY,
    from_date DATE NOT NULL,
    to_date DATE NOT NULL,
    initial_cash NUMERIC(20, 8) NOT NULL,
    minimum_history_days INTEGER NOT NULL CHECK (minimum_history_days BETWEEN 50 AND 500),
    total_symbols INTEGER NOT NULL CHECK (total_symbols > 0),
    completed_symbols INTEGER NOT NULL DEFAULT 0,
    successful_rows INTEGER NOT NULL DEFAULT 0,
    failed_rows INTEGER NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_backtest_performance_report_jobs_status
    ON backtest_performance_report_jobs (status, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS uq_backtest_performance_report_jobs_active
    ON backtest_performance_report_jobs ((1))
    WHERE status IN ('QUEUED', 'RUNNING');

CREATE TABLE IF NOT EXISTS backtest_performance_report_items (
    job_id BIGINT NOT NULL REFERENCES backtest_performance_report_jobs(id) ON DELETE CASCADE,
    symbol VARCHAR(40) NOT NULL,
    strategy_type VARCHAR(40) NOT NULL,
    model VARCHAR(40) NOT NULL DEFAULT '',
    status VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    run_id BIGINT REFERENCES backtest_runs(id),
    total_return_pct NUMERIC(20, 8),
    mdd_pct NUMERIC(20, 8),
    mae NUMERIC(20, 8),
    rmse NUMERIC(20, 8),
    mae_pct NUMERIC(20, 8),
    rmse_pct NUMERIC(20, 8),
    buy_signal_count INTEGER,
    hold_signal_count INTEGER,
    sell_signal_count INTEGER,
    trade_count INTEGER,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, symbol, strategy_type, model)
);

CREATE INDEX IF NOT EXISTS idx_backtest_performance_report_items_job
    ON backtest_performance_report_items (job_id, symbol);
