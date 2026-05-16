-- 추가 기술적 지표 컬럼
-- 볼린저 밴드, 스토캐스틱, ATR, OBV

-- 볼린저 밴드 (Middle은 MA20 재사용)
ALTER TABLE symbol_daily_indicators
    ADD COLUMN IF NOT EXISTS bb_upper NUMERIC(24, 8),  -- 상단 밴드 (MA20 + 2σ)
    ADD COLUMN IF NOT EXISTS bb_lower NUMERIC(24, 8);  -- 하단 밴드 (MA20 - 2σ)

-- 스토캐스틱
ALTER TABLE symbol_daily_indicators
    ADD COLUMN IF NOT EXISTS stoch_k NUMERIC(8, 4),    -- %K (14일)
    ADD COLUMN IF NOT EXISTS stoch_d NUMERIC(8, 4);    -- %D (%K의 3일 SMA)

-- ATR (Average True Range)
ALTER TABLE symbol_daily_indicators
    ADD COLUMN IF NOT EXISTS atr14 NUMERIC(24, 8);     -- 14일 ATR

-- OBV (On Balance Volume)
ALTER TABLE symbol_daily_indicators
    ADD COLUMN IF NOT EXISTS obv BIGINT;               -- 누적 거래량 기반 지표

COMMENT ON COLUMN symbol_daily_indicators.bb_upper IS '볼린저 밴드 상단 (MA20 + 2 * 20일 표준편차)';
COMMENT ON COLUMN symbol_daily_indicators.bb_lower IS '볼린저 밴드 하단 (MA20 - 2 * 20일 표준편차)';
COMMENT ON COLUMN symbol_daily_indicators.stoch_k IS '스토캐스틱 %K (14일 기준)';
COMMENT ON COLUMN symbol_daily_indicators.stoch_d IS '스토캐스틱 %D (%K의 3일 SMA)';
COMMENT ON COLUMN symbol_daily_indicators.atr14 IS 'Average True Range (14일)';
COMMENT ON COLUMN symbol_daily_indicators.obv IS 'On Balance Volume (누적)';
