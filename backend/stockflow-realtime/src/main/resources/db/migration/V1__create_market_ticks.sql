-- 정규화 체결 틱 (NormalizedTradeDTO). TimescaleDB 이미지에서도 동일 DDL 사용.
-- 하이퍼테이블 전환(선택): DB에 접속해 아래를 한 번 실행하면 청크 파티셔닝 활성화.
--   SELECT create_hypertable('market_ticks', 'ts',
--       chunk_time_interval => INTERVAL '1 day',
--       if_not_exists => TRUE);
CREATE TABLE market_ticks (
    id          BIGSERIAL,
    source      VARCHAR(64)  NOT NULL,
    symbol      VARCHAR(32)  NOT NULL,
    trade_id    VARCHAR(128) NOT NULL,
    price       NUMERIC(24, 8) NOT NULL,
    volume      NUMERIC(24, 8) NOT NULL,
    exchange    VARCHAR(32)  NOT NULL,
    ts          TIMESTAMPTZ  NOT NULL,
    received_at TIMESTAMPTZ  NOT NULL,
    market_type VARCHAR(16)  NOT NULL,
    ingested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, ts)
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_ts ON market_ticks (symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_market_ticks_source_trade ON market_ticks (source, trade_id, ts DESC);
