-- TimescaleDB: 시계열 청크 파티셔닝 (일 단위).
-- 일반 PostgreSQL이면 확장이 없어 이 마이그레이션은 아무 작업도 하지 않는다.
DO $body$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE NOTICE 'TimescaleDB extension not found; skip create_hypertable for market_ticks';
        RETURN;
    END IF;

    EXECUTE $ts$
        SELECT create_hypertable(
            'market_ticks',
            'ts',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE,
            migrate_data => TRUE
        );
    $ts$;
END $body$;
