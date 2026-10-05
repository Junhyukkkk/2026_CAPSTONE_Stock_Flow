-- /api/predictions/{symbol}/compare 응답(모델별·시점별 예측값) 이력. 새 테이블만 생성한다.
CREATE TABLE IF NOT EXISTS prediction_runs (
    id            BIGSERIAL PRIMARY KEY,
    symbol        VARCHAR(32)      NOT NULL,
    "interval"    VARCHAR(8)       NOT NULL,
    horizon       INT              NOT NULL,
    source        VARCHAR(32)      NOT NULL DEFAULT '',
    base_ts       TIMESTAMPTZ      NOT NULL,
    base_value    DOUBLE PRECISION,
    requested_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    latency_ms    INT,
    raw_models    JSONB,
    UNIQUE (symbol, "interval", horizon, source, base_ts)
);

CREATE INDEX IF NOT EXISTS idx_prediction_runs_symbol_interval_base_ts
    ON prediction_runs (symbol, "interval", base_ts DESC);

CREATE TABLE IF NOT EXISTS prediction_forecast_points (
    run_id      BIGINT           NOT NULL REFERENCES prediction_runs (id) ON DELETE CASCADE,
    model       VARCHAR(64)      NOT NULL,
    ts          TIMESTAMPTZ      NOT NULL,
    yhat        DOUBLE PRECISION NOT NULL,
    yhat_lower  DOUBLE PRECISION,
    yhat_upper  DOUBLE PRECISION,
    trained_at  TIMESTAMPTZ,
    mae         DOUBLE PRECISION,
    rmse        DOUBLE PRECISION,
    PRIMARY KEY (run_id, model, ts)
);

CREATE INDEX IF NOT EXISTS idx_prediction_forecast_points_ts
    ON prediction_forecast_points (ts);
