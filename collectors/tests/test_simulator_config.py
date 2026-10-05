"""Config 의 SIM_* 설정과 validate_simulator() 검증 테스트"""
import importlib

import pytest

_SIM_KEYS = (
    "SIM_SYMBOLS_FILE", "SIM_TOTAL_TPS", "SIM_MARKET_HOURS", "SIM_SEED",
    "SIM_DRY_RUN", "SIM_TICK_INTERVAL_MS", "SIM_PRICE_SOURCE", "SIM_RATE_MODE", "SIM_RATE_SCALE",
    "SIM_SOURCE_LABEL",
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
    assert Config.SIM_SOURCE_LABEL == "SIMULATOR"
    assert Config.SIM_RATE_MODE == "realistic"
    assert Config.SIM_RATE_SCALE == 1.0
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
    ({"SIM_RATE_MODE": "foo"}, "SIM_RATE_MODE"),
    ({"SIM_RATE_SCALE": "0"}, "SIM_RATE_SCALE"),
    ({"SIM_RATE_SCALE": "-0.5"}, "SIM_RATE_SCALE"),
    ({"SIM_RATE_SCALE": "101"}, "SIM_RATE_SCALE"),
    ({"SIM_RATE_SCALE": "100.5"}, "SIM_RATE_SCALE"),
    ({"SIM_RATE_SCALE": "nan"}, "SIM_RATE_SCALE"),
    ({"SIM_RATE_SCALE": "abc"}, "SIM_RATE_SCALE"),
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
    ({"SIM_TICK_INTERVAL_MS": "2001"}, "SIM_TICK_INTERVAL_MS"),
    ({"SIM_PRICE_SOURCE": "bloomberg"}, "SIM_PRICE_SOURCE"),
    ({"SIM_SOURCE_LABEL": "simload"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "SIM LOAD"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": " SIMLOAD"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "SIMLOAD\n"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "SIM-LOAD"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "A" * 33}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "BINANCE"}, "SIM_SOURCE_LABEL"),
    ({"SIM_SOURCE_LABEL": "ALPACA"}, "SIM_SOURCE_LABEL"),
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


def test_tick_interval_upper_bound_is_inclusive(monkeypatch):
    assert _load_config(monkeypatch, SIM_TICK_INTERVAL_MS="2000").validate_simulator() is True


def test_rate_mode_and_scale_overrides(monkeypatch):
    Config = _load_config(monkeypatch, SIM_RATE_MODE=" Fixed ", SIM_RATE_SCALE="10")
    assert Config.SIM_RATE_MODE == "fixed"
    assert Config.SIM_RATE_SCALE == 10.0
    assert Config.validate_simulator() is True
    assert _load_config(monkeypatch, SIM_RATE_SCALE="0.25").SIM_RATE_SCALE == 0.25


def test_rate_scale_upper_bound_is_100_inclusive(monkeypatch):
    assert _load_config(monkeypatch, SIM_RATE_SCALE="100").validate_simulator() is True
    assert _load_config(monkeypatch, SIM_RATE_SCALE="100.01").validate_simulator() is False
    assert _load_config(monkeypatch, SIM_RATE_SCALE="101").validate_simulator() is False


@pytest.mark.parametrize("label", ["SIMLOAD", "SIM_LOAD_2", "A", "X" * 32])
def test_valid_source_labels_accepted(monkeypatch, label):
    Config = _load_config(monkeypatch, SIM_SOURCE_LABEL=label)
    assert Config.SIM_SOURCE_LABEL == label
    assert Config.validate_simulator() is True


def test_default_source_label_matches_generator_source(monkeypatch):
    from simulator.generator import SOURCE
    assert _load_config(monkeypatch).SIM_SOURCE_LABEL == SOURCE


def test_empty_source_label_falls_back_to_default(monkeypatch):
    Config = _load_config(monkeypatch, SIM_SOURCE_LABEL="")
    assert Config.SIM_SOURCE_LABEL == "SIMULATOR"
    assert Config.validate_simulator() is True


def test_invalid_source_label_does_not_break_shared_validate(monkeypatch):
    Config = _load_config(monkeypatch, SIM_SOURCE_LABEL="bad label")
    assert Config.validate() is True
    assert Config.validate_simulator() is False
