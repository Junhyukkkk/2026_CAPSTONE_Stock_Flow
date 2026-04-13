-- 데모 API·운영 모니터링에서 흔한 패턴: 전역 최신 틱 (ORDER BY ts DESC LIMIT n).
-- (symbol, ts) 인덱스만으로는 부족할 수 있어 ts 단일 축 보조 인덱스 추가.
CREATE INDEX IF NOT EXISTS idx_market_ticks_ts_desc ON market_ticks (ts DESC);

COMMENT ON INDEX idx_market_ticks_ts_desc IS '최근 틱 시간순 조회·근사 행 수 외 스캔 완화용.';
