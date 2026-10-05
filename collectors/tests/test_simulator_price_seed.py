"""시작가 로더 테스트 — 네트워크 없이 http_get 을 주입해 검증"""
import os

import pytest
import requests

from simulator import price_seed
from simulator.price_seed import seed_prices
from simulator.universe import Instrument, load_universe

INSTRUMENTS = [
    Instrument("AAA", 100.0, 0.25, 5.0),
    Instrument("BBB", 50.0, 0.30, 3.0),
    Instrument("CCC", 20.0, 0.40, 1.0),
]
KEYS = dict(api_key="key", api_secret="secret")


class FakeResponse:
    def __init__(self, status_code=200, body=None, json_error=None):
        self.status_code = status_code
        self._body = body
        self._json_error = json_error

    def json(self):
        if self._json_error:
            raise self._json_error
        return self._body


class FakeHttp:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers, "timeout": timeout})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def prices(instruments):
    return {i.symbol: i.price for i in instruments}


def snapshot(trade=None, daily=None, prev=None):
    snap = {}
    if trade is not None:
        snap["latestTrade"] = {"p": trade}
    if daily is not None:
        snap["dailyBar"] = {"c": daily}
    if prev is not None:
        snap["prevDailyBar"] = {"c": prev}
    return snap


def test_static_returns_csv_prices_without_any_request():
    http = FakeHttp()
    assert seed_prices(INSTRUMENTS, "static", **KEYS, http_get=http) == INSTRUMENTS
    assert http.calls == []


def test_alpaca_success_overwrites_csv_prices_and_keeps_other_fields():
    http = FakeHttp(FakeResponse(body={
        "AAA": snapshot(trade=103.5), "BBB": snapshot(trade=48.25), "CCC": snapshot(trade=21.0),
    }))
    result = seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http, timeout=3.0)

    assert prices(result) == {"AAA": 103.5, "BBB": 48.25, "CCC": 21.0}
    assert [(i.symbol, i.volatility, i.weight) for i in result] == [(i.symbol, i.volatility, i.weight) for i in INSTRUMENTS]
    call = http.calls[0]
    assert call["url"] == "https://data.alpaca.markets/v2/stocks/snapshots"
    assert call["params"] == {"symbols": "AAA,BBB,CCC", "feed": "iex"}
    assert call["headers"] == {"APCA-API-KEY-ID": "key", "APCA-API-SECRET-KEY": "secret"}
    assert call["timeout"] == 3.0


def test_default_timeout_is_set():
    http = FakeHttp(FakeResponse(body={}))
    seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http)
    assert http.calls[0]["timeout"] == price_seed.REQUEST_TIMEOUT_SEC > 0


def test_price_field_fallback_order_latest_trade_then_daily_then_prev_daily():
    http = FakeHttp(FakeResponse(body={
        "AAA": snapshot(trade=101.0, daily=102.0, prev=103.0),
        "BBB": snapshot(daily=51.0, prev=52.0),
        "CCC": snapshot(prev=22.0),
    }))
    assert prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http)) == {"AAA": 101.0, "BBB": 51.0, "CCC": 22.0}


def test_missing_symbols_keep_only_their_csv_price():
    http = FakeHttp(FakeResponse(body={"AAA": snapshot(trade=110.0), "CCC": {}}))
    assert prices(seed_prices(INSTRUMENTS, "auto", **KEYS, http_get=http)) == {"AAA": 110.0, "BBB": 50.0, "CCC": 20.0}


def test_wrapped_snapshots_response_is_supported():
    http = FakeHttp(FakeResponse(body={"snapshots": {"AAA": snapshot(trade=99.0)}}))
    assert prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http))["AAA"] == 99.0


@pytest.mark.parametrize("response", [
    FakeResponse(401, {"message": "unauthorized"}),
    FakeResponse(403, {"message": "forbidden"}),
    FakeResponse(500, {"message": "boom"}),
    FakeResponse(200, {}),
    FakeResponse(200, None),
    FakeResponse(200, ["unexpected"]),
    FakeResponse(200, "text"),
    FakeResponse(200, {"AAA": "not-a-dict", "BBB": ["x"], "CCC": {"latestTrade": "oops"}}),
    FakeResponse(200, json_error=ValueError("not json")),
    requests.exceptions.ConnectTimeout("timed out"),
    requests.exceptions.ConnectionError("down"),
    RuntimeError("anything"),
])
def test_failures_fall_back_to_csv_without_raising(response):
    http = FakeHttp(response)
    assert seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http) == INSTRUMENTS


@pytest.mark.parametrize("bad", [0, -5.0, 100.0 * 5.01, 100.0 * 0.19, "101", True, None, float("nan"), float("inf")])
def test_unrealistic_or_invalid_prices_are_ignored(bad):
    http = FakeHttp(FakeResponse(body={"AAA": snapshot(trade=bad)}))
    assert prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http))["AAA"] == 100.0


def test_price_bounds_are_inclusive_0_2x_to_5x():
    http = FakeHttp(FakeResponse(body={"AAA": snapshot(trade=500.0), "BBB": snapshot(trade=10.0)}))
    result = prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http))
    assert result["AAA"] == 500.0 and result["BBB"] == 10.0


def test_unrealistic_latest_trade_falls_through_to_sane_daily_bar():
    http = FakeHttp(FakeResponse(body={"AAA": snapshot(trade=9999.0, daily=104.0)}))
    assert prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http))["AAA"] == 104.0


def test_auto_without_keys_does_not_call_alpaca():
    http = FakeHttp()
    assert seed_prices(INSTRUMENTS, "auto", None, None, http_get=http) == INSTRUMENTS
    assert seed_prices(INSTRUMENTS, "auto", "key", None, http_get=http) == INSTRUMENTS
    assert seed_prices(INSTRUMENTS, "auto", "", "secret", http_get=http) == INSTRUMENTS
    assert http.calls == []


def test_alpaca_without_keys_falls_back_without_request():
    http = FakeHttp()
    assert seed_prices(INSTRUMENTS, "alpaca", None, None, http_get=http) == INSTRUMENTS
    assert http.calls == []


def test_symbols_are_requested_in_chunks_and_a_failed_chunk_only_loses_itself(monkeypatch):
    monkeypatch.setattr(price_seed, "CHUNK_SIZE", 2)
    http = FakeHttp(requests.exceptions.ReadTimeout("slow"), FakeResponse(body={"CCC": snapshot(trade=21.0)}))
    result = seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http)

    assert [c["params"]["symbols"] for c in http.calls] == ["AAA,BBB", "CCC"]
    assert prices(result) == {"AAA": 100.0, "BBB": 50.0, "CCC": 21.0}


def test_auth_failure_stops_after_first_chunk(monkeypatch):
    monkeypatch.setattr(price_seed, "CHUNK_SIZE", 1)
    http = FakeHttp(FakeResponse(401, {}), FakeResponse(401, {}), FakeResponse(401, {}))
    assert seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http) == INSTRUMENTS
    assert len(http.calls) == 1


def test_response_symbol_case_is_normalized():
    http = FakeHttp(FakeResponse(body={"aaa": snapshot(trade=101.0)}))
    assert prices(seed_prices(INSTRUMENTS, "alpaca", **KEYS, http_get=http))["AAA"] == 101.0


def test_bundled_universe_is_well_formed():
    path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulator", "universe.csv")
    with open(path, encoding="utf-8") as f:
        data_rows = len(f.read().strip().splitlines()) - 1
    instruments = load_universe(path)

    assert len(instruments) == data_rows  # 건너뛴 행 없음, 중복 없음
    assert 90 <= len(instruments) <= 120
    weights = [i.weight for i in instruments]
    assert max(weights) / min(weights) <= 50
    assert all(i.price > 0 and 0.1 <= i.volatility <= 0.7 for i in instruments)
    symbols = {i.symbol for i in instruments}
    assert {"AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "SPY", "QQQ", "IWM", "DIA", "XLK", "XLF"} <= symbols
