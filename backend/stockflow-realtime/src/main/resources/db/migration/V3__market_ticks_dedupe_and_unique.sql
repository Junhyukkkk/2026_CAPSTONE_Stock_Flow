-- 동일 (source, trade_id, ts) 중복 행 제거 후 멱등 INSERT용 유니크 인덱스.
-- 하이퍼테이블/일반 테이블 모두 동작.
DELETE FROM market_ticks m
USING (
    SELECT id,
           ts,
           ROW_NUMBER() OVER (
               PARTITION BY source, trade_id, ts
               ORDER BY id ASC
           ) AS rn
    FROM market_ticks
) d
WHERE m.id = d.id
  AND m.ts = d.ts
  AND d.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_market_ticks_source_trade_ts
    ON market_ticks (source, trade_id, ts);
