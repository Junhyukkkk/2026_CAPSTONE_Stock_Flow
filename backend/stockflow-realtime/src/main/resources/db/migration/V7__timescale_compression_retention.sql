-- TimescaleDB: 원시 틱 압춡·보존 + 1분 연속 집계 압춡·보존.
-- 일반 PostgreSQL 또는 하이퍼테이블 미생성 시 스킵.
--
-- 보존 기준(업계에서 흔한 완급 조합):
--   market_ticks     : 7일 경과 청크 압춡, 400일 초과 원시 틱 청크 삭제 (핫 데이터는 비압축)
--   market_ticks_1m  : 30일 경과 압춡, 1095일(3년) 초과 1분봉 삭제

DO $body$
DECLARE
    ht_ticks boolean;
    ca_1m    boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE NOTICE 'TimescaleDB extension not found; skip compression and retention policies';
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public'
          AND hypertable_name = 'market_ticks'
    )
    INTO ht_ticks;

    IF NOT ht_ticks THEN
        RAISE NOTICE 'market_ticks is not a hypertable; skip lifecycle policies';
        RETURN;
    END IF;

    -- 압춡 설정: segmentby 는 유니크 인덱스(uq_market_ticks_symbol_source_trade_ts)와 정합(V6).
    EXECUTE $c$
        ALTER TABLE market_ticks SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'symbol, source',
            timescaledb.compress_orderby = 'ts DESC, trade_id'
        )
    $c$;

    PERFORM add_compression_policy('market_ticks', INTERVAL '7 days');
    PERFORM add_retention_policy('market_ticks', INTERVAL '400 days');

    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = 'public'
          AND view_name = 'market_ticks_1m'
    )
    INTO ca_1m;

    IF NOT ca_1m THEN
        RAISE NOTICE 'market_ticks_1m not found; skip CA lifecycle';
        RETURN;
    END IF;

    BEGIN
        EXECUTE $ca$
            ALTER MATERIALIZED VIEW market_ticks_1m SET (timescaledb.compress = true)
        $ca$;
        PERFORM add_compression_policy('market_ticks_1m', INTERVAL '30 days');
        PERFORM add_retention_policy('market_ticks_1m', INTERVAL '1095 days');
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'market_ticks_1m compression/retention skipped: %', SQLERRM;
    END;
END $body$;
