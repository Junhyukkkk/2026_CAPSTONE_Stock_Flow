-- 1분봉 OHLCV 연속 집계 (TimescaleDB 전용, 하이퍼테이블 필요).
DO $body$
DECLARE
    ca_exists boolean;
    ht_exists boolean;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE NOTICE 'TimescaleDB extension not found; skip market_ticks_1m continuous aggregate';
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = 'public'
          AND hypertable_name = 'market_ticks'
    )
    INTO ht_exists;

    IF NOT ht_exists THEN
        RAISE NOTICE 'market_ticks is not a hypertable; skip continuous aggregate';
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_schema = 'public'
          AND view_name = 'market_ticks_1m'
    )
    INTO ca_exists;

    IF ca_exists THEN
        RAISE NOTICE 'Continuous aggregate market_ticks_1m already exists';
        RETURN;
    END IF;

    EXECUTE $ca$
        CREATE MATERIALIZED VIEW market_ticks_1m
        WITH (timescaledb.continuous) AS
        SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket,
               symbol,
               source,
               first(price, ts)  AS open,
               max(price)        AS high,
               min(price)        AS low,
               last(price, ts)   AS close,
               sum(volume)       AS volume
        FROM market_ticks
        GROUP BY time_bucket(INTERVAL '1 minute', ts), symbol, source
        WITH NO DATA;
    $ca$;

    CALL refresh_continuous_aggregate(
        continuous_aggregate => 'market_ticks_1m',
        window_start         => NULL,
        window_end           => NULL
    );

    PERFORM add_continuous_aggregate_policy(
        'market_ticks_1m',
        start_offset      => INTERVAL '3 hours',
        end_offset        => INTERVAL '1 minute',
        schedule_interval => INTERVAL '1 minute'
    );
END $body$;
