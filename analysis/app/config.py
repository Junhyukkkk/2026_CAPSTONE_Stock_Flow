"""환경변수 기반 설정.

로컬 실행 시 호스트의 TimescaleDB(localhost:5433)를, 컨테이너 실행 시
infra_default 네트워크의 timescaledb:5432 를 바라보도록 환경변수로 전환한다.
"""
import os

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:  # python-dotenv 미설치여도 동작
    pass


def _get(name: str, default: str) -> str:
    return os.getenv(name, default)


class Settings:
    def __init__(self) -> None:
        self.db_host = _get("DB_HOST", "localhost")
        self.db_port = int(_get("DB_PORT", "5433"))
        self.db_name = _get("DB_NAME", "stockflow")
        self.db_user = _get("DB_USERNAME", "postgres")
        self.db_password = _get("DB_PASSWORD", "postgres")

        self.model_dir = _get("MODEL_DIR", "models")
        self.default_interval = _get("DEFAULT_INTERVAL", "1m")  # 1m | 1d
        self.default_limit = int(_get("DEFAULT_LIMIT", "2000"))

    @property
    def db_url(self) -> str:
        return (
            f"postgresql+psycopg2://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )


settings = Settings()
