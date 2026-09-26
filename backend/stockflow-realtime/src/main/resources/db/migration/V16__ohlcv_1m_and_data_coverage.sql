-- 1분봉 캔들 저장소(ohlcv_1m)와 데이터 커버리지 점검 테이블.
--
-- market_ticks_1m(연속 집계)은 수집된 틱에서만 만들어지므로 과거 캔들을 넣을 수 없고,
-- 수집이 끊긴 분은 비거나 일부만 집계된다. ohlcv_1m 은 세 경로를 한 곳에 모은다.
--   - LIVE     : market-data-sync 가 market_ticks_1m 의 확정된 분을 1분마다 복사
--   - EXCHANGE : 거래소 공식 캔들(초기 백필 · 일일 확정 · 결측 복구). LIVE 를 덮어쓰고, LIVE 가 덮어쓰지 못한다.

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    symbol       VARCHAR(32)    NOT NULL,
    source       VARCHAR(64)    NOT NULL,
    bucket       TIMESTAMPTZ    NOT NULL,
    open         NUMERIC(24, 8) NOT NULL,
    high         NUMERIC(24, 8) NOT NULL,
    low          NUMERIC(24, 8) NOT NULL,
    close        NUMERIC(24, 8) NOT NULL,
    volume       NUMERIC(32, 8) NOT NULL,
    quote_volume NUMERIC(32, 8),
    trade_count  BIGINT,
    origin       VARCHAR(16)    NOT NULL
        CONSTRAINT chk_ohlcv_1m_origin CHECK (origin IN ('EXCHANGE', 'LIVE')),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, source, bucket)
);

COMMENT ON TABLE ohlcv_1m IS '1분봉 캔들. 실시간(LIVE)과 거래소 확정본(EXCHANGE)을 함께 보관한다.';
COMMENT ON COLUMN ohlcv_1m.origin IS 'EXCHANGE=거래소 공식 캔들(우선), LIVE=수집 틱 집계(임시)';

DO $body$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE NOTICE 'TimescaleDB extension not found; ohlcv_1m stays a plain table';
        RETURN;
    END IF;

    PERFORM create_hypertable('ohlcv_1m', 'bucket',
                              chunk_time_interval => INTERVAL '7 days',
                              if_not_exists => TRUE,
                              migrate_data => TRUE);

    EXECUTE $c$
        ALTER TABLE ohlcv_1m SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol, source',
            timescaledb.compress_orderby = 'bucket DESC'
        )
    $c$;
    -- 일일 확정(전날)과 48시간 결측 복구가 끝난 뒤에 압축되도록 여유를 둔다.
    PERFORM add_compression_policy('ohlcv_1m', INTERVAL '7 days', if_not_exists => TRUE);
END
$body$;

-- 일봉에도 출처를 기록한다. 기존 행(배치 집계 · 기존 백필)은 NULL 로 남는다.
ALTER TABLE symbol_daily_ohlcv ADD COLUMN IF NOT EXISTS origin VARCHAR(16);

CREATE TABLE IF NOT EXISTS data_coverage (
    symbol          VARCHAR(32) NOT NULL,
    source          VARCHAR(64) NOT NULL,
    interval_code   VARCHAR(8)  NOT NULL
        CONSTRAINT chk_data_coverage_interval CHECK (interval_code IN ('1m', '1d')),
    first_ts        TIMESTAMPTZ,
    last_ts         TIMESTAMPTZ,
    row_count       BIGINT,
    missing_count   BIGINT,
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          VARCHAR(16) NOT NULL DEFAULT 'UNKNOWN'
        CONSTRAINT chk_data_coverage_status CHECK (status IN ('UNKNOWN', 'COMPLETE', 'GAPS', 'BACKFILLING')),
    note            TEXT,
    PRIMARY KEY (symbol, source, interval_code)
);

COMMENT ON TABLE data_coverage IS '종목별 캔들 보유 범위와 결측 수. market-data-sync 가 점검 후 갱신한다.';
