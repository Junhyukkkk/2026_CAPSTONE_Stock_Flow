"""학습된 모델/메타데이터 저장·로드 (joblib 파일 기반)."""
import os

import joblib

from ..config import settings


def _path(symbol: str, interval: str) -> str:
    safe = symbol.replace("/", "_").replace(os.sep, "_")
    return os.path.join(settings.model_dir, f"{safe}__{interval}.joblib")


def save_model(symbol: str, interval: str, payload: dict) -> str:
    os.makedirs(settings.model_dir, exist_ok=True)
    path = _path(symbol, interval)
    joblib.dump(payload, path)
    return path


def load_model(symbol: str, interval: str):
    path = _path(symbol, interval)
    if not os.path.exists(path):
        return None
    return joblib.load(path)
