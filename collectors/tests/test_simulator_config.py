"""Config 의 SIM_* 설정과 validate_simulator() 검증 테스트"""
import importlib

import pytest

_SIM_KEYS = (
    "SIM_SYMBOLS_FILE", "SIM_TOTAL_TPS", "SIM_MARKET_HOURS", "SIM_SEED",
    "SIM_DRY_RUN", "SIM_TICK_INTERVAL_MS", "SIM_PRICE_SOURCE",
)


def _load_config(monkeypatch, **env):
    for key in _SIM_KEYS:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *a, **k: False)
    import config
    return importlib.reload(config).Config


def test_defaults(monkeypatch):
    Config = _load_config(monkeypatch)
    assert Config.SIM_SYMBOLS_FILE == "simulator/universe.csv"
    assert Config.SIM_TOTAL_TPS == 300
    assert Config.SIM_MARKET_HOURS == "always"
    assert Config.SIM_SEED is None
    assert Config.SIM_DRY_RUN is False
    assert Config.SIM_TICK_INTERVAL_MS == 50
    assert Config.SIM_PRICE_SOURCE == "static"
    assert Config.validate_simulator() is True


def test_valid_overrides(monkeypatch):
    Config = _load_config(
        monkeypatch, SIM_TOTAL_TPS="1500.5", SIM_MARKET_HOURS="US", SIM_SEED="7",
        SIM_DRY_RUN="True", SIM_TICK_INTERVAL_MS="20", SIM_PRICE_SOURCE="auto",
    )
    assert Config.SIM_TOTAL_TPS == 1500.5
    assert Config.SIM_MARKET_HOURS == "us"
    assert Config.SIM_SEED == 7
    assert Config.SIM_DRY_RUN is True
    assert Config.SIM_TICK_INTERVAL_MS == 20
    assert Config.validate_simulator() is True


@pytest.mark.parametrize("env, expected_in_message", [
    ({"SIM_TOTAL_TPS": "abc"}, "SIM_TOTAL_TPS"),
    ({"SIM_TOTAL_TPS": "0"}, "SIM_TOTAL_TPS"),
    ({"SIM_TOTAL_TPS": "-5"}, "SIM_TOTAL_TPS"),
    ({"SIM_TOTAL_TPS": "nan"}, "SIM_TOTAL_TPS"),
    ({"SIM_TOTAL_TPS": "inf"}, "SIM_TOTAL_TPS"),
    ({"SIM_MARKET_HOURS": "nyse"}, "SIM_MARKET_HOURS"),
    ({"SIM_SEED": "x1"}, "SIM_SEED"),
    ({"SIM_DRY_RUN": "ture"}, "SIM_DRY_RUN"),
    ({"SIM_TICK_INTERVAL_MS": "0"}, "SIM_TICK_INTERVAL_MS"),
    ({"SIM_TICK_INTERVAL_MS": "fast"}, "SIM_TICK_INTERVAL_MS"),
    ({"SIM_PRICE_SOURCE": "bloomberg"}, "SIM_PRICE_SOURCE"),
])
def test_invalid_values_fail_validation_with_clear_message(monkeypatch, capsys, env, expected_in_message):
    Config = _load_config(monkeypatch, **env)  # 잘못된 값이어도 import 는 죽지 않는다
    assert Config.validate_simulator() is False
    out = capsys.readouterr().out
    assert "설정 오류" in out and expected_in_message in out


def test_multiple_errors_are_all_reported(monkeypatch, capsys):
    Config = _load_config(monkeypatch, SIM_TOTAL_TPS="-1", SIM_MARKET_HOURS="x", SIM_DRY_RUN="maybe")
    assert Config.validate_simulator() is False
    out = capsys.readouterr().out
    assert out.count("설정 오류") == 3


def test_invalid_sim_env_does_not_break_regular_validate(monkeypatch):
    """SIM_* 오류가 Binance/Alpaca 수집기의 기동(Config.validate)에 영향을 주지 않는다"""
    Config = _load_config(monkeypatch, SIM_TOTAL_TPS="abc")
    assert Config.validate() is True
