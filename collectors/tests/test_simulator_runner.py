"""StockSimulator(스케줄러/장 시간/전송) 단위 테스트 — Kafka 없이 가짜 시계·프로듀서 사용"""
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import stock_simulator
from simulator import market_clock
from simulator.market_clock import is_us_market_open
from simulator.universe import Instrument

NY = ZoneInfo("America/New_York")
INSTRUMENTS = [Instrument(f"S{i}", 100.0 + i, 0.3, 1.0 + i) for i in range(10)]


def ny(y, m, d, hh, mm, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=NY).timestamp()


class FakeClock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now


class FakeProducer:
    def __init__(self, accept=True):
        self.accept = accept
        self.messages = []

    def produce(self, topic, key, value, callback=None):
        self.messages.append((topic, key, value))
        return self.accept

    def log_stats(self):
        pass


def _simulator(clock, **kwargs):
    kwargs.setdefault("total_tps", 300.0)
    kwargs.setdefault("seed", 1)
    return stock_simulator.StockSimulator(INSTRUMENTS, clock=clock, **kwargs)


def _drive(sim, clock, seconds, tick=0.05):
    trades = []
    for _ in range(int(seconds / tick)):
        clock.now += tick
        trades.extend(sim.step())
    return trades


def test_is_us_market_open_boundaries():
    assert is_us_market_open(ny(2026, 10, 5, 9, 30))          # 월요일 개장 시각
    assert is_us_market_open(ny(2026, 10, 5, 15, 59, 59))
    assert not is_us_market_open(ny(2026, 10, 5, 9, 29, 59))
    assert not is_us_market_open(ny(2026, 10, 5, 16, 0))
    assert not is_us_market_open(ny(2026, 10, 10, 11, 0))     # 토요일
    assert not is_us_market_open(ny(2026, 10, 11, 11, 0))     # 일요일


def test_us_mode_emits_nothing_outside_hours_and_trades_during_hours():
    for closed in (ny(2026, 10, 5, 8, 0), ny(2026, 10, 5, 17, 0), ny(2026, 10, 10, 11, 0)):
        clock = FakeClock(closed)
        sim = _simulator(clock, market_hours="us")
        assert _drive(sim, clock, 5) == []
        assert sim.market_open is False

    clock = FakeClock(ny(2026, 10, 5, 10, 0))
    sim = _simulator(clock, market_hours="us")
    trades = _drive(sim, clock, 5)
    assert 1000 < len(trades) < 2000  # 300 TPS * 5 s
    assert sim.market_open is True


def test_us_mode_never_emits_trades_stamped_outside_hours_around_open_and_close():
    open_ms = ny(2026, 10, 5, 9, 30) * 1000
    close_ms = ny(2026, 10, 5, 16, 0) * 1000

    clock = FakeClock(ny(2026, 10, 5, 9, 29, 55))
    sim = _simulator(clock, market_hours="us")
    trades = _drive(sim, clock, 10)
    assert trades and all(t["timestamp"] >= open_ms for t in trades)

    clock = FakeClock(ny(2026, 10, 5, 15, 59, 55))
    sim = _simulator(clock, market_hours="us")
    trades = _drive(sim, clock, 10)
    assert trades and all(t["timestamp"] < close_ms for t in trades)


def test_always_mode_ignores_market_hours():
    clock = FakeClock(ny(2026, 10, 10, 3, 0))  # 토요일 새벽
    sim = _simulator(clock, market_hours="always")
    assert len(_drive(sim, clock, 2)) > 100


def test_long_stall_is_capped_instead_of_bursting():
    clock = FakeClock(1_760_000_000.0)
    sim = _simulator(clock)
    clock.now += 3600  # 1시간 정지
    trades = sim.step()
    assert len(trades) < 300 * stock_simulator.MAX_CATCHUP_SEC * 1.5
    assert all(t["timestamp"] >= (clock.now - stock_simulator.MAX_CATCHUP_SEC) * 1000 - 1 for t in trades)


def test_dry_run_prints_one_json_line_per_trade(capsys, tmp_path, monkeypatch):
    monkeypatch.setattr(stock_simulator.Config, "HEALTH_FILE_PATH", str(tmp_path / "health.json"))
    clock = FakeClock(1_760_000_000.0)
    sim = _simulator(clock, dry_run=True)
    clock.now += 1
    trades = sim.step()
    sim._emit(trades)

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == len(trades) > 0
    assert [json.loads(line) for line in lines] == trades
    assert (tmp_path / "health.json").exists()


def test_kafka_mode_produces_keyed_by_symbol_and_records_health(tmp_path, monkeypatch):
    health = tmp_path / "health.json"
    monkeypatch.setattr(stock_simulator.Config, "HEALTH_FILE_PATH", str(health))
    clock = FakeClock(1_760_000_000.0)
    producer = FakeProducer()
    sim = _simulator(clock, producer=producer)
    clock.now += 1
    trades = sim.step()
    sim._emit(trades)

    assert len(producer.messages) == len(trades) > 0
    for (topic, key, value), trade in zip(producer.messages, trades):
        assert topic == stock_simulator.Config.KAFKA_TOPIC_NAME
        assert key == trade["symbol"] and value is trade
    assert "last_success_epoch" in json.loads(health.read_text())


def test_rejected_produce_is_counted_and_does_not_touch_health(tmp_path, monkeypatch):
    health = tmp_path / "health.json"
    monkeypatch.setattr(stock_simulator.Config, "HEALTH_FILE_PATH", str(health))
    clock = FakeClock(1_760_000_000.0)
    sim = _simulator(clock, producer=FakeProducer(accept=False))
    clock.now += 1
    trades = sim.step()
    sim._emit(trades)

    assert sim.rejected == len(trades) > 0
    assert not health.exists()


def test_same_seed_same_prices_but_trade_ids_differ_across_simultaneous_starts():
    """결정성은 가격/수량/시각/seq 로 유지하되, 같은 ms 에 동시에 기동한 두 실행의 tradeId 는 겹치지 않는다"""
    def run():
        clock = FakeClock(1_760_000_000.0)
        sim = _simulator(clock, seed=42)
        clock.now += 1
        return sim.step()

    a, b = run(), run()
    assert len(a) > 100

    def strip(trades):
        return [(t["symbol"], t["price"], t["volume"], t["timestamp"], t["tradeId"].rsplit("-", 1)[1]) for t in trades]

    assert strip(a) == strip(b)
    assert not {t["tradeId"] for t in a} & {t["tradeId"] for t in b}
    assert re.fullmatch(r"SIM-S\d-[0-9a-z]{4,}-\d+", a[0]["tradeId"])


def test_missing_tz_database_raises_clear_error(monkeypatch):
    def not_found(name):
        raise market_clock.ZoneInfoNotFoundError(name)

    market_clock._eastern.cache_clear()
    monkeypatch.setattr(market_clock, "ZoneInfo", not_found)
    try:
        with pytest.raises(RuntimeError, match="tzdata"):
            is_us_market_open(1_760_000_000.0)
    finally:
        market_clock._eastern.cache_clear()
