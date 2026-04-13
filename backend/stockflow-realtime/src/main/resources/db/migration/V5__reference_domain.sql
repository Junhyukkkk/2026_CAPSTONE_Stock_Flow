-- 참조·도메인 스키마: 종목 마스터, 사용자, 관심종목.
-- market_ticks 는 초당 대량 INSERT 이므로 instruments FK 를 두지 않는다(애플리케이션/배치로 정합성 유지).

CREATE TABLE instruments (
    symbol       VARCHAR(32) PRIMARY KEY,
    market_type  VARCHAR(16)  NOT NULL
        CONSTRAINT chk_instruments_market_type CHECK (market_type IN ('CRYPTO', 'STOCK')),
    exchange     VARCHAR(32)  NOT NULL DEFAULT 'UNKNOWN',
    name         VARCHAR(256),
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_instruments_market_active
    ON instruments (market_type, is_active)
    WHERE is_active;

CREATE INDEX IF NOT EXISTS idx_instruments_last_seen
    ON instruments (last_seen_at DESC);

COMMENT ON TABLE instruments IS '거래 가능 종목 마스터. 틱 테이블과는 논리적 참조만(무 FK).';
COMMENT ON COLUMN instruments.symbol IS '대문자 정규화 권장 (앱/Kafka 키와 동일).';

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         VARCHAR(320) NOT NULL,
    password_hash VARCHAR(255),
    display_name  VARCHAR(128),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_users_email_lower ON users ((lower(trim(email))));

COMMENT ON TABLE users IS '서비스 사용자(로그인/즐겨찾기). 비밀번호는 해시만 저장.';
COMMENT ON COLUMN users.password_hash IS 'bcrypt/argon2 등. OAuth 전용이면 NULL 허용.';

CREATE TABLE user_watchlist (
    user_id    UUID         NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    symbol     VARCHAR(32)  NOT NULL REFERENCES instruments (symbol) ON DELETE CASCADE,
    sort_order INTEGER      NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, symbol)
);

CREATE INDEX IF NOT EXISTS idx_user_watchlist_symbol ON user_watchlist (symbol);

COMMENT ON TABLE user_watchlist IS '사용자 관심종목. symbol 은 instruments 에 사전 등록된 값만 허용.';

CREATE OR REPLACE FUNCTION trg_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql AS $f$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$f$;

CREATE TRIGGER tr_instruments_updated_at
    BEFORE UPDATE ON instruments
    FOR EACH ROW
    EXECUTE FUNCTION trg_set_updated_at();

CREATE TRIGGER tr_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION trg_set_updated_at();

-- 수집 파이프라인·API 에서 신규 심볼을 안전하게 등록할 때 사용.
CREATE OR REPLACE FUNCTION register_instrument(
    p_symbol      VARCHAR(32),
    p_market_type VARCHAR(16),
    p_exchange    VARCHAR(32),
    p_name        VARCHAR(256) DEFAULT NULL
) RETURNS VOID
LANGUAGE plpgsql AS $f$
DECLARE
    v_sym VARCHAR(32) := upper(trim(p_symbol));
    v_ex  VARCHAR(32) := upper(trim(p_exchange));
BEGIN
    IF v_sym IS NULL OR v_sym = '' THEN
        RAISE EXCEPTION 'symbol required';
    END IF;
    INSERT INTO instruments (symbol, market_type, exchange, name, last_seen_at)
    VALUES (
        v_sym,
        p_market_type,
        COALESCE(NULLIF(v_ex, ''), 'UNKNOWN'),
        COALESCE(NULLIF(trim(p_name), ''), v_sym),
        NOW()
    )
    ON CONFLICT (symbol) DO UPDATE SET
        market_type  = EXCLUDED.market_type,
        exchange     = EXCLUDED.exchange,
        name         = COALESCE(NULLIF(EXCLUDED.name, ''), instruments.name),
        last_seen_at = NOW(),
        updated_at   = NOW();
END;
$f$;

COMMENT ON FUNCTION register_instrument IS '종목 마스터 upsert. Kafka consumer / REST 에서 호출 가능.';
