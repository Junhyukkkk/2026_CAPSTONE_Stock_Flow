-- Timescale 압축(segmentby)과 정합되도록 멱등 키에 symbol 포함.
-- 기존 (source, trade_id, ts) 만으로는 동일 source 내 trade_id 충돌 가능성이 있어 운영·압축 정책 모두에 symbol 이 포함되는 편이 안전하다.

DELETE FROM market_ticks m
USING (
    SELECT id,
           ts,
           ROW_NUMBER() OVER (
               PARTITION BY symbol, source, trade_id, ts
               ORDER BY id ASC
           ) AS rn
    FROM market_ticks
) d
WHERE m.id = d.id
  AND m.ts = d.ts
  AND d.rn > 1;

DROP INDEX IF EXISTS uq_market_ticks_source_trade_ts;

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_ticks_symbol_source_trade_ts
    ON market_ticks (symbol, source, trade_id, ts);

COMMENT ON INDEX uq_market_ticks_symbol_source_trade_ts IS 'Bulk INSERT ON CONFLICT 및 Timescale compress_segmentby(symbol, source) 정합.';
