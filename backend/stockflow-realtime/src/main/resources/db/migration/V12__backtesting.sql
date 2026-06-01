-- 백테스팅: 전략 정의, 실행 결과, 체결 내역, 자산 곡선.
-- 입력 데이터는 symbol_daily_ohlcv (배치 집계 일봉)을 사용한다.

-- 전략 정의 (CRUD 대상). params 는 전략 타입별 파라미터(JSONB).
CREATE TABLE backtest_strategies (
    id            BIGSERIAL    PRIMARY KEY,
    name          VARCHAR(128) NOT NULL,
    symbol        VARCHAR(32)  NOT NULL,
    strategy_type VARCHAR(32)  NOT NULL
        CONSTRAINT chk_backtest_strategy_type CHECK (
            strategy_type IN ('BUY_AND_HOLD', 'MA_CROSSOVER', 'RSI')
        ),
    params        JSONB         NOT NULL DEFAULT '{}'::jsonb,
    initial_cash  NUMERIC(24, 8) NOT NULL DEFAULT 10000,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_strategies_symbol
    ON backtest_strategies (symbol);

CREATE TRIGGER tr_backtest_strategies_updated_at
    BEFORE UPDATE ON backtest_strategies
    FOR EACH ROW
    EXECUTE FUNCTION trg_set_updated_at();

COMMENT ON TABLE backtest_strategies IS '백테스팅 전략 정의. params 는 전략 타입별 파라미터(JSONB).';

-- 백테스트 실행 결과(요약 지표). strategy_id 는 ad-hoc 실행 시 NULL 가능.
CREATE TABLE backtest_runs (
    id               BIGSERIAL   PRIMARY KEY,
    strategy_id      BIGINT      REFERENCES backtest_strategies (id) ON DELETE SET NULL,
    symbol           VARCHAR(32) NOT NULL,
    strategy_type    VARCHAR(32) NOT NULL,
    params           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    from_date        DATE        NOT NULL,
    to_date          DATE        NOT NULL,
    initial_cash     NUMERIC(24, 8) NOT NULL,
    final_equity     NUMERIC(24, 8),
    total_return_pct NUMERIC(16, 6),     -- 누적 수익률 (%)
    cagr_pct         NUMERIC(16, 6),     -- 연평균 복리 수익률 (%)
    mdd_pct          NUMERIC(16, 6),     -- 최대 낙폭 Max Drawdown (%)
    trade_count      INTEGER,            -- 완료된 매매(라운드트립) 수
    win_rate_pct     NUMERIC(8, 4),      -- 승률 (%)
    bar_count        INTEGER,            -- 시뮬레이션에 사용된 일봉 수
    status           VARCHAR(16) NOT NULL
        CONSTRAINT chk_backtest_run_status CHECK (status IN ('SUCCESS', 'FAILED')),
    error_summary    TEXT,
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy
    ON backtest_runs (strategy_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_symbol
    ON backtest_runs (symbol, created_at DESC);

COMMENT ON TABLE backtest_runs IS '백테스트 실행 요약 결과(누적 수익률/MDD/승률 등).';
COMMENT ON COLUMN backtest_runs.trade_count IS '완료된 라운드트립(매수→매도) 수.';

-- 체결 내역 (리포트/시각화용).
CREATE TABLE backtest_trades (
    id           BIGSERIAL    PRIMARY KEY,
    run_id       BIGINT       NOT NULL REFERENCES backtest_runs (id) ON DELETE CASCADE,
    seq          INTEGER      NOT NULL,
    trade_date   DATE         NOT NULL,
    side         VARCHAR(8)   NOT NULL
        CONSTRAINT chk_backtest_trade_side CHECK (side IN ('BUY', 'SELL')),
    price        NUMERIC(24, 8) NOT NULL,
    quantity     NUMERIC(24, 8) NOT NULL,
    cash_after   NUMERIC(24, 8) NOT NULL,
    equity_after NUMERIC(24, 8) NOT NULL,
    pnl_pct      NUMERIC(16, 6)            -- SELL 시 직전 매수가 대비 손익률(%)
);

CREATE INDEX IF NOT EXISTS idx_backtest_trades_run
    ON backtest_trades (run_id, seq);

COMMENT ON TABLE backtest_trades IS '백테스트 매수/매도 체결 내역.';

-- 자산 곡선 (시각화용). 일자별 평가금액 및 낙폭.
CREATE TABLE backtest_equity_curve (
    run_id       BIGINT       NOT NULL REFERENCES backtest_runs (id) ON DELETE CASCADE,
    trade_date   DATE         NOT NULL,
    equity       NUMERIC(24, 8) NOT NULL,
    drawdown_pct NUMERIC(16, 6) NOT NULL,
    PRIMARY KEY (run_id, trade_date)
);

COMMENT ON TABLE backtest_equity_curve IS '백테스트 일자별 평가금액·낙폭(시각화용).';
