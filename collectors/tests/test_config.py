"""
config.py 단위 테스트

수집기는 소스(Binance/Alpaca)와 무관하게 정규화 토픽 하나(market.normalized)로만
전송한다는 설계(KAFKA_TOPIC_DESIGN.md / backend/infra/docker-compose.yml)를
Config 기본값이 그대로 반영하는지 확인한다.
"""
import importlib

import pytest

_TOPIC_ENV_KEYS = ("KAFKA_TOPIC_NAME", "BINANCE_TOPIC_NAME", "ALPACA_TOPIC_NAME")


def _load_config(monkeypatch, **env):
    """토픽 관련 환경변수를 지운 뒤 주어진 env만 설정하고 config 모듈을 다시 로드한다."""
    for key in _TOPIC_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import config
    return importlib.reload(config).Config


def test_default_topic_is_the_normalized_topic(monkeypatch):
    """아무 설정이 없으면 Consumer가 구독하는 market.normalized 로 보낸다"""
    Config = _load_config(monkeypatch)
    assert Config.KAFKA_TOPIC_NAME == "market.normalized"


def test_topic_can_be_overridden_by_env(monkeypatch):
    Config = _load_config(monkeypatch, KAFKA_TOPIC_NAME="market.test")
    assert Config.KAFKA_TOPIC_NAME == "market.test"


def test_per_source_topic_settings_are_gone(monkeypatch):
    """소스별 토픽 변수는 설계에 없으므로 Config 에서 제거됐어야 한다"""
    Config = _load_config(monkeypatch)
    assert not hasattr(Config, "BINANCE_TOPIC_NAME")
    assert not hasattr(Config, "ALPACA_TOPIC_NAME")


def test_validate_passes_with_defaults(monkeypatch):
    """예전의 '두 소스 토픽이 같으면 거부' 검증이 사라져 기본 설정으로 기동 가능해야 한다"""
    Config = _load_config(monkeypatch)
    assert Config.validate() is True
