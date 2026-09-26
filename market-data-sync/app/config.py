"""환경변수 기반 설정.

컨테이너에서는 infra_default 네트워크의 timescaledb:5432 를,
호스트에서 직접 실행할 때는 localhost:5433 을 바라보도록 환경변수로 전환한다.
"""
import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # python-dotenv 미설치여도 동작
    pass


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


class Settings:
    def __init__(self) -> None:
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = _int("DB_PORT", 5433)
        self.db_name = os.getenv("DB_NAME", "stockflow")
        self.db_user = os.getenv("DB_USERNAME", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "postgres")

        # 실시간 동기화: market_ticks_1m → ohlcv_1m (LIVE)
        self.live_sync_interval_seconds = _int("LIVE_SYNC_INTERVAL_SECONDS", 60)
        # 연속 집계는 1분마다 now()-1분까지 갱신되므로, 2분 전까지를 확정된 분으로 본다.
        self.live_sync_settle_minutes = _int("LIVE_SYNC_SETTLE_MINUTES", 2)
        self.live_sync_lookback_minutes = _int("LIVE_SYNC_LOOKBACK_MINUTES", 15)
        # 연속 집계 갱신 정책의 start_offset(3시간) 안에서 늦게 들어온 틱을 따라잡는 넓은 동기화
        self.live_sync_wide_lookback_minutes = _int("LIVE_SYNC_WIDE_LOOKBACK_MINUTES", 190)

        # 결측 복구: 최근 N시간(그 이전은 일일 확정이 하루 단위로 덮어씀), 동기화 중인 최근 몇 분은 제외
        self.repair_lookback_hours = _int("REPAIR_LOOKBACK_HOURS", 6)
        self.repair_settle_minutes = _int("REPAIR_SETTLE_MINUTES", 10)
        # 일일 확정: Spring 일봉 배치(01:05 UTC) 이후에 돌도록 01:30 UTC
        self.confirm_hour_utc = _int("CONFIRM_HOUR_UTC", 1)
        self.confirm_minute_utc = _int("CONFIRM_MINUTE_UTC", 30)

        # Binance 호출 간격과 요청 가중치 상한(분당 6000 중 일부만 사용)
        self.archive_pause_seconds = float(os.getenv("ARCHIVE_PAUSE_SECONDS", "0.3"))
        self.rest_pause_seconds = float(os.getenv("REST_PAUSE_SECONDS", "0.2"))
        self.binance_weight_soft_limit = _int("BINANCE_WEIGHT_SOFT_LIMIT", 3000)

        self.log_level = os.getenv("LOG_LEVEL", "INFO")

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()
