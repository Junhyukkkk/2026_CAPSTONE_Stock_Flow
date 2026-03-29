-- market_ticks 적재 상태 점검 (TimescaleDB 컨테이너 기준)
-- 예: docker exec -i stockflow-timescaledb psql -U postgres -d stockflow < infra/sql/health_market_ticks.sql

\echo '=== 행 수 ==='
SELECT COUNT(*) AS market_ticks_rows FROM market_ticks;

\echo '=== 최근 체결 시각 (지연 감지) ==='
SELECT MAX(ts) AS last_trade_ts,
       NOW()   AS server_now,
       NOW() - MAX(ts) AS lag_from_last_tick
FROM market_ticks;

\echo '=== 소스별 건수 (상위 10) ==='
SELECT source, COUNT(*) AS cnt
FROM market_ticks
GROUP BY source
ORDER BY cnt DESC
LIMIT 10;

\echo '=== 심볼별 건수 (상위 10) ==='
SELECT symbol, COUNT(*) AS cnt
FROM market_ticks
GROUP BY symbol
ORDER BY cnt DESC
LIMIT 10;

\echo '=== 최근 5건 샘플 ==='
SELECT source, symbol, price, volume, trade_id, ts
FROM market_ticks
ORDER BY ts DESC
LIMIT 5;

\echo '=== Timescale 메타 (확장 없으면 이 블록만 실패할 수 있음) ==='
DO $h$
DECLARE
    n bigint;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE NOTICE 'timescaledb extension 없음: 하이퍼테이블/1분봉 집계 점검 생략';
        RETURN;
    END IF;

    RAISE NOTICE 'hypertable market_ticks: %',
        (SELECT COUNT(*)::text FROM timescaledb_information.hypertables
         WHERE hypertable_schema = 'public' AND hypertable_name = 'market_ticks');

    IF to_regclass('public.market_ticks_1m') IS NOT NULL THEN
        SELECT COUNT(*) INTO n FROM market_ticks_1m;
        RAISE NOTICE 'market_ticks_1m rows: %', n;
    ELSE
        RAISE NOTICE 'market_ticks_1m 없음 (Flyway V4 미적용 또는 비-Timescale)';
    END IF;
END $h$;
