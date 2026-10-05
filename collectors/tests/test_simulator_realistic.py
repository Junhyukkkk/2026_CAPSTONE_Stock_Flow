"""SIM_RATE_MODE=realistic: 실제 체결량 기반 발생률 + 장중 강도 곡선 단위 테스트 (Kafka 불필요)"""
import csv
import math
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import stock_simulator
from simulator.generator import DAILY_TRADES_PER_WEIGHT, SESSION_SECONDS, TradeGenerator
from simulator.market_clock import SESSION_MINUTES, intraday_profile, us_intraday_profile
from simulator.universe import Instrument, load_universe

NY = ZoneInfo("America/New_York")
UNIVERSE_CSV = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulator", "universe.csv")

INSTRUMENTS = [
    Instrument("AAA", 100.0, 0.25, 1.0, 234_000.0),   # 10 TPS
    Instrument("BBB", 20.0, 0.40, 2.0, 117_000.0),    # 5 TPS
    Instrument("CCC", 500.0, 0.20, 7.0, 46_800.0),    # 2 TPS (weight 순서와 daily_trades 순서가 달라도 daily_trades 를 따른다)
]
EXPECTED_TPS = sum(i.daily_trades for i in INSTRUMENTS) / SESSION_SECONDS  # 17


def ny(y, m, d, hh, mm, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=NY).timestamp()


def _gen(instruments=INSTRUMENTS, seed=1, **kwargs):
    kwargs.setdefault("rate_mode", "realistic")
    return TradeGenerator(instruments, 99999.0, run_id="r", seed=seed, **kwargs)


def _run(generator, seconds, t0, tick=0.5):
    trades = []
    for i in range(int(round(seconds / tick))):
        trades.extend(generator.generate(t0 + i * tick, t0 + (i + 1) * tick))
    return trades


def _counts(trades):
    counts = {}
    for t in trades:
        counts[t.symbol] = counts.get(t.symbol, 0) + 1
    return counts


# (a) 총 발생률 = Σ daily_trades / 23400 * scale (SIM_TOTAL_TPS 는 무시)
def test_total_rate_matches_sum_of_daily_trades_and_ignores_total_tps():
    generator = _gen(market_hours="always")
    assert generator.expected_tps == pytest.approx(EXPECTED_TPS)

    trades = _run(generator, 200, t0=1_760_000_000.0)
    assert abs(len(trades) - EXPECTED_TPS * 200) < 0.04 * EXPECTED_TPS * 200
    counts = _counts(trades)
    for inst in INSTRUMENTS:
        expected = inst.daily_trades / SESSION_SECONDS * 200
        assert abs(counts[inst.symbol] - expected) < 0.12 * expected, (inst.symbol, counts)


# (b) SIM_RATE_SCALE 선형 반영
def test_rate_scale_is_linear():
    t0 = 1_760_000_000.0
    base = _gen(rate_scale=1.0, market_hours="always")
    half = _gen(rate_scale=0.5, market_hours="always")
    triple = _gen(rate_scale=3.0, market_hours="always")
    assert half.expected_tps == pytest.approx(base.expected_tps * 0.5)
    assert triple.expected_tps == pytest.approx(base.expected_tps * 3.0)

    n_base = len(_run(base, 200, t0))
    n_half = len(_run(half, 200, t0))
    n_triple = len(_run(triple, 200, t0))
    assert n_half / n_base == pytest.approx(0.5, rel=0.08)
    assert n_triple / n_base == pytest.approx(3.0, rel=0.08)


# (c) U자 곡선: 정규화·모양
def test_profile_averages_exactly_one_over_session():
    # 구간별 선형이고 꼭짓점이 정수 분이라 1분 중점 합이 적분과 정확히 같다
    mean = sum(intraday_profile(m + 0.5) for m in range(int(SESSION_MINUTES))) / SESSION_MINUTES
    assert mean == pytest.approx(1.0, abs=1e-12)
    # 초 단위 격자로도 확인
    fine = sum(intraday_profile((s + 0.5) / 60) for s in range(int(SESSION_MINUTES * 60))) / (SESSION_MINUTES * 60)
    assert fine == pytest.approx(1.0, abs=1e-9)


def test_profile_is_u_shaped():
    open_burst, after_30, noon, close_start, close_end = (
        intraday_profile(0.0), intraday_profile(30.0), intraday_profile(150.0),
        intraday_profile(360.0), intraday_profile(389.99),
    )
    assert 2.5 < open_burst < 3.3          # 개장 직후 평균의 약 3배
    assert 1.2 < after_30 < 1.8            # 30분 뒤 약 1.5배로 감쇠
    assert 0.45 < noon < 0.7               # 정오 무렵 최저
    assert 1.0 < close_start < 1.6         # 마감 30분 전부터 상승
    assert 2.0 < close_end < 2.8           # 마감 직전 약 2.5배
    assert open_burst > after_30 > noon < close_start < close_end
    assert min(intraday_profile(m / 2) for m in range(780)) == pytest.approx(noon, rel=0.01)
    assert intraday_profile(-0.01) == 0.0 and intraday_profile(SESSION_MINUTES) == 0.0


def test_us_profile_is_zero_outside_session_and_weekends():
    assert us_intraday_profile(ny(2026, 10, 5, 9, 29, 59)) == 0.0
    assert us_intraday_profile(ny(2026, 10, 5, 16, 0)) == 0.0
    assert us_intraday_profile(ny(2026, 10, 5, 3, 0)) == 0.0
    assert us_intraday_profile(ny(2026, 10, 10, 11, 0)) == 0.0   # 토요일
    assert us_intraday_profile(ny(2026, 10, 5, 9, 30)) == pytest.approx(intraday_profile(0.0))
    assert us_intraday_profile(ny(2026, 10, 5, 12, 0)) == pytest.approx(intraday_profile(150.0))
    assert us_intraday_profile(ny(2026, 10, 5, 9, 31)) > us_intraday_profile(ny(2026, 10, 5, 12, 0))
    # 서머타임 전환 전후에도 같은 현지 시각이면 같은 값
    assert us_intraday_profile(ny(2026, 3, 9, 9, 45)) == pytest.approx(us_intraday_profile(ny(2026, 3, 6, 9, 45)))


def test_us_mode_trade_count_follows_profile_and_is_zero_after_hours():
    generator = _gen(market_hours="us")
    first_half_hour = len(_run(generator, 300, ny(2026, 10, 5, 9, 30)))
    noon = len(_run(generator, 300, ny(2026, 10, 5, 12, 0)))
    assert first_half_hour > 2.5 * noon
    mean_open = sum(intraday_profile(s / 60) for s in range(0, 300)) / 300
    assert first_half_hour == pytest.approx(EXPECTED_TPS * 300 * mean_open, rel=0.05)

    assert _run(generator, 60, ny(2026, 10, 5, 8, 0)) == []
    assert _run(generator, 60, ny(2026, 10, 5, 16, 30)) == []
    assert _run(generator, 60, ny(2026, 10, 10, 11, 0)) == []


def test_us_mode_session_total_matches_daily_trades():
    # 세션 전체를 10초 격자로 돌린 총 건수 ≈ Σ daily_trades (곡선 평균 1.0)
    generator = _gen([Instrument("AAA", 100.0, 0.25, 1.0, 100_000.0)], market_hours="us")
    t0 = ny(2026, 10, 5, 9, 30)
    total = sum(len(generator.generate(t0 + i * 10, t0 + (i + 1) * 10)) for i in range(int(SESSION_SECONDS / 10)))
    assert total == pytest.approx(100_000, rel=0.02)


def test_always_mode_is_flat_24h_without_profile():
    generator = _gen(market_hours="always")
    rate = 17.0
    hours = {}
    for label, (h, m) in {"night": (3, 0), "open": (9, 30), "noon": (12, 0), "evening": (20, 0)}.items():
        hours[label] = len(_run(generator, 400, ny(2026, 10, 5, h, m), tick=1.0)) / 400
    for label, tps in hours.items():
        assert tps == pytest.approx(rate, rel=0.08), (label, hours)
    # 주말에도 생성
    assert len(_run(generator, 10, ny(2026, 10, 10, 11, 0))) > 0


# (d) fixed 모드는 이전 동작 그대로 — SIM_TOTAL_TPS·weight 만 사용, daily_trades·강도 곡선·scale 무시
def test_fixed_mode_ignores_daily_trades_scale_and_profile():
    t0 = ny(2026, 10, 5, 9, 30)
    plain = [Instrument(i.symbol, i.price, i.volatility, i.weight) for i in INSTRUMENTS]
    a = TradeGenerator(INSTRUMENTS, 40.0, run_id="r", seed=5, rate_mode="fixed", rate_scale=7.0, market_hours="us")
    b = TradeGenerator(plain, 40.0, run_id="r", seed=5)
    assert a.expected_tps == pytest.approx(40.0)
    ta = [t.to_dict() for t in _run(a, 20, t0)]
    tb = [t.to_dict() for t in _run(b, 20, t0)]
    assert ta == tb and len(ta) > 400
    assert _run(a, 5, ny(2026, 10, 10, 11, 0)) != []  # 장외에서도 generator 는 fixed 에서 곡선을 쓰지 않는다


# (e) daily_trades 누락 행은 weight 로 폴백
def test_missing_daily_trades_falls_back_to_weight():
    mixed = [Instrument("AAA", 100.0, 0.25, 3.0, None), Instrument("BBB", 50.0, 0.3, 1.0, 234_000.0)]
    generator = _gen(mixed, market_hours="always")
    assert generator.expected_tps == pytest.approx((3.0 * DAILY_TRADES_PER_WEIGHT + 234_000.0) / SESSION_SECONDS)
    counts = _counts(_run(generator, 100, 1_760_000_000.0))
    assert counts["AAA"] / counts["BBB"] == pytest.approx(3.0 * DAILY_TRADES_PER_WEIGHT / 234_000.0, rel=0.15)


def test_universe_csv_has_realistic_daily_trades_for_every_symbol():
    instruments = load_universe(UNIVERSE_CSV)
    assert len(instruments) == 105
    assert all(i.daily_trades and i.daily_trades >= 30_000 for i in instruments)
    by_symbol = {i.symbol: i.daily_trades for i in instruments}
    assert by_symbol["SPY"] == 1_200_000 and by_symbol["QQQ"] == 900_000 and by_symbol["IWM"] == 400_000
    assert max(by_symbol, key=by_symbol.get) in ("NVDA", "TSLA")
    total_tps = sum(by_symbol.values()) / SESSION_SECONDS
    assert 500 <= total_tps <= 1100
    with open(UNIVERSE_CSV, newline="") as f:
        assert next(csv.reader(f)) == ["symbol", "price", "volatility", "weight", "daily_trades"]


# (g) 결정성
def test_realistic_mode_is_deterministic_with_seed():
    t0 = ny(2026, 10, 5, 9, 30)
    for hours in ("us", "always"):
        a = [t.to_dict() for t in _run(_gen(seed=42, market_hours=hours), 10, t0)]
        b = [t.to_dict() for t in _run(_gen(seed=42, market_hours=hours), 10, t0)]
        c = [t.to_dict() for t in _run(_gen(seed=43, market_hours=hours), 10, t0)]
        assert len(a) > 100 and a == b and a != c


def test_stock_simulator_passes_rate_settings_through():
    class Clock:
        now = ny(2026, 10, 5, 12, 0)

        def __call__(self):
            return self.now

    clock = Clock()
    sim = stock_simulator.StockSimulator(
        INSTRUMENTS, total_tps=1.0, market_hours="always", seed=1,
        rate_mode="realistic", rate_scale=2.0, clock=clock,
    )
    assert sim.expected_tps == pytest.approx(EXPECTED_TPS * 2.0)
    trades = []
    for _ in range(200):
        clock.now += 0.05
        trades.extend(sim.step())
    assert 0.8 * EXPECTED_TPS * 2 * 10 < len(trades) < 1.2 * EXPECTED_TPS * 2 * 10
    assert {t["source"] for t in trades} == {"SIMULATOR"} and {t["exchange"] for t in trades} == {"SIM"}


# 변동성: 체결 수·scale·모드와 무관하게 일간 수익률 표준편차 ≈ volatility / sqrt(252)
def _daily_return_std(market_hours, daily_trades, rate_scale, volatility, days, seed):
    generator = TradeGenerator(
        [Instrument("VOL", 100.0, volatility, 1.0, daily_trades)], 1.0, run_id="v", seed=seed,
        rate_mode="realistic", rate_scale=rate_scale, market_hours=market_hours,
    )
    day = datetime(2026, 1, 5, tzinfo=NY)  # 월요일부터 평일만
    prev_log, returns = math.log(100.0), []
    tick = 30.0 if market_hours == "us" else 120.0
    span = SESSION_SECONDS if market_hours == "us" else 86400.0
    while len(returns) < days:
        if day.weekday() < 5:
            start = (day + timedelta(hours=9, minutes=30)).timestamp() if market_hours == "us" else day.timestamp()
            last = None
            for i in range(int(span / tick)):
                for t in generator.generate(start + i * tick, start + (i + 1) * tick):
                    last = float(t.price)
            if last is not None:
                returns.append(math.log(last) - prev_log)
                prev_log += returns[-1]
        day += timedelta(days=1)
    mean = sum(returns) / len(returns)
    return math.sqrt(sum((r - mean) ** 2 for r in returns) / (len(returns) - 1))


@pytest.mark.parametrize("market_hours, daily_trades, rate_scale", [
    ("us", 600.0, 1.0),       # 정규장 기준
    ("us", 2400.0, 0.25),     # scale 로 체결 수를 줄여도 일 변동성 유지
    ("always", 600.0, 1.0),   # 24시간 흘러도 일 변동성 유지(보정 전에는 약 1.9배)
])
def test_daily_return_std_stays_near_volatility_over_sqrt_252(market_hours, daily_trades, rate_scale):
    volatility = 0.30
    std = _daily_return_std(market_hours, daily_trades, rate_scale, volatility, days=140, seed=11)
    target = volatility / math.sqrt(252)
    # 평균회귀(하루 약 -20%)가 소폭 깎고 점프가 소폭 더하므로 ±25% 안이면 충분 (보정이 틀리면 1.9배·0.5배로 벗어난다)
    assert 0.75 * target < std < 1.25 * target, (std, target)
