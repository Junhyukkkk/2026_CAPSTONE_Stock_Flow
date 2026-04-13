-- 배치·분석 파이프라인용 테이블 (Job/지표 로직은 이후 Phase에서 채움).

CREATE TABLE batch_job_runs (
    id               BIGSERIAL PRIMARY KEY,
    job_name         VARCHAR(128) NOT NULL,
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(32)  NOT NULL
        CONSTRAINT chk_batch_job_runs_status CHECK (
            status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')
        ),
    tickers_processed INTEGER,
    rows_written      BIGINT,
    error_summary     TEXT,
    meta              JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_job_runs_job_name_started
    ON batch_job_runs (job_name, started_at DESC);

COMMENT ON TABLE batch_job_runs IS 'Spring Batch/스케줄러 실행 이력. 장애·성능 추적용.';

-- 일봉 OHLCV: 배치 Job이 market_ticks 또는 연속 집계로부터 채움. 지표·백테스트 입력 계층.
CREATE TABLE symbol_daily_ohlcv (
    symbol      VARCHAR(32)  NOT NULL,
    trade_date  DATE         NOT NULL,
    market_type VARCHAR(16)  NOT NULL
        CONSTRAINT chk_symbol_daily_ohlcv_market_type CHECK (market_type IN ('CRYPTO', 'STOCK')),
    source      VARCHAR(64)  NOT NULL,
    open        NUMERIC(24, 8) NOT NULL,
    high        NUMERIC(24, 8) NOT NULL,
    low         NUMERIC(24, 8) NOT NULL,
    close       NUMERIC(24, 8) NOT NULL,
    volume      NUMERIC(24, 8) NOT NULL,
    tick_count  BIGINT,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date, source)
);

CREATE INDEX IF NOT EXISTS idx_symbol_daily_ohlcv_trade_date
    ON symbol_daily_ohlcv (trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_symbol_daily_ohlcv_symbol_date
    ON symbol_daily_ohlcv (symbol, trade_date DESC);

COMMENT ON TABLE symbol_daily_ohlcv IS '일봉 OHLCV 스냅샷. 실시간 틱/1분 CA와 별도 보관.';
