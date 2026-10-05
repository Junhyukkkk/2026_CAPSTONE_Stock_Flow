"""simulator.generator / price_model 단위 테스트 (Kafka 불필요)"""
import os
import random
import re
from decimal import Decimal

from normalizer import NormalizedTradeDTO
from simulator.generator import TradeGenerator, to_base36
from simulator.price_model import poisson
from simulator.universe import Instrument, load_universe

DTO_KEYS = {"source", "symbol", "price", "volume", "tradeId", "exchange", "timestamp", "receivedAt", "marketType"}
T0 = 1_760_000_000.0
UNIVERSE_CSV = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulator", "universe.csv")

INSTRUMENTS = [
    Instrument("AAA", 100.0, 0.25, 1.0),
    Instrument("BBB", 20.0, 0.40, 2.0),
    Instrument("CCC", 500.0, 0.20, 7.0),
]


def _run(generator, seconds, tick=0.05, t0=T0):
    trades = []
    steps = int(round(seconds / tick))
    for i in range(steps):
        trades.extend(generator.generate(t0 + i * tick, t0 + (i + 1) * tick))
    return trades


def _generator(seed=1, total_tps=300.0, instruments=INSTRUMENTS):
    return TradeGenerator(instruments, total_tps, run_id="abc", seed=seed)


def test_same_seed_gives_same_sequence():
    a = [t.to_dict() for t in _run(_generator(seed=42), 5)]
    b = [t.to_dict() for t in _run(_generator(seed=42), 5)]
    c = [t.to_dict() for t in _run(_generator(seed=43), 5)]
    assert len(a) > 100
    assert a == b
    assert a != c


def test_prices_stay_in_cent_units_and_realistic_range_over_a_week():
    start = {i.symbol: i.price for i in INSTRUMENTS}
    # 낮은 TPS 로 1주일(1시간 구간 168개)을 돌린다
    generator = _generator(seed=7, total_tps=0.5)
    max_dev = {s: 0.0 for s in start}
    count = 0
    for hour in range(7 * 24):
        for t in generator.generate(T0 + hour * 3600, T0 + (hour + 1) * 3600):
            count += 1
            assert t.price >= Decimal("0.01")
            assert t.price == t.price.quantize(Decimal("0.01"))
            assert str(t.price) == "{:.2f}".format(t.price)
            max_dev[t.symbol] = max(max_dev[t.symbol], abs(float(t.price) / start[t.symbol] - 1))
    assert count > 100_000
    for symbol, dev in max_dev.items():
        assert dev < 0.40, (symbol, dev)


def test_mean_reversion_keeps_normal_vol_stock_near_start_price():
    # 클램프 안전장치가 아니라 평균회귀 자체가 범위를 잡는지 확인 (변동성 0.25, 1주일)
    generator = TradeGenerator([Instrument("AAA", 100.0, 0.25, 1.0)], 0.5, run_id="x", seed=3)
    prices = []
    for hour in range(7 * 24):
        prices.extend(float(t.price) for t in generator.generate(T0 + hour * 3600, T0 + (hour + 1) * 3600))
    assert max(abs(p / 100.0 - 1) for p in prices) < 0.20


def test_penny_stock_floor_and_extreme_volatility():
    generator = TradeGenerator([Instrument("PNY", 0.05, 3.0, 1.0)], 5.0, run_id="x", seed=5)
    prices = [t.price for t in _run(generator, 600, tick=1.0)]
    assert prices and min(prices) >= Decimal("0.01")


def test_volumes_are_small_integers_with_rare_blocks():
    generator = _generator(seed=11, total_tps=2000.0)
    volumes = [int(t.volume) for t in _run(generator, 30)]
    assert len(volumes) > 50_000
    assert all(v >= 1 for v in volumes)
    blocks = [v for v in volumes if v > 500]
    assert all(1000 <= v <= 20000 for v in blocks)
    assert 0.003 < len(blocks) / len(volumes) < 0.03
    assert sorted(volumes)[len(volumes) // 2] < 100  # 중앙값은 작은 수량


def test_trade_counts_follow_weights_and_total_tps():
    total_tps = 1000.0
    seconds = 60
    trades = _run(_generator(seed=9, total_tps=total_tps), seconds)

    assert abs(len(trades) - total_tps * seconds) < 0.03 * total_tps * seconds
    counts = {s: 0 for s in ("AAA", "BBB", "CCC")}
    for t in trades:
        counts[t.symbol] += 1
    total = len(trades)
    for symbol, share in (("AAA", 0.1), ("BBB", 0.2), ("CCC", 0.7)):
        assert abs(counts[symbol] / total - share) < 0.02, (symbol, counts)


def test_timestamps_monotonic_per_symbol_and_trade_ids_unique():
    trades = _run(_generator(seed=2, total_tps=500.0), 20)
    last_ts = {}
    for t in trades:
        assert t.timestamp >= last_ts.get(t.symbol, 0)
        last_ts[t.symbol] = t.timestamp
        assert T0 * 1000 <= t.timestamp <= (T0 + 20) * 1000
    ids = [t.trade_id for t in trades]
    assert len(set(ids)) == len(ids)
    assert ids[0].startswith("SIM-") and "-abc-" in ids[0]
    # 한 호출 안에서는 종목 구분 없이 시간 순으로 정렬돼 나온다
    batch = _generator(seed=2, total_tps=5000.0).generate(T0, T0 + 1)
    assert [t.timestamp for t in batch] == sorted(t.timestamp for t in batch)


def test_per_symbol_sequence_is_consecutive_and_restart_changes_ids():
    first = _run(_generator(seed=1), 3)
    seqs = [int(t.trade_id.rsplit("-", 1)[1]) for t in first if t.symbol == "CCC"]
    assert seqs == list(range(1, len(seqs) + 1))

    restarted = _run(TradeGenerator(INSTRUMENTS, 300.0, run_id="abd", seed=1), 3)
    assert not {t.trade_id for t in first} & {t.trade_id for t in restarted}


def test_output_matches_dto_contract():
    trades = _run(_generator(seed=1), 2)
    assert trades
    for t in trades:
        assert isinstance(t, NormalizedTradeDTO)
        d = t.to_dict()
        assert set(d) == DTO_KEYS
        assert d["marketType"] == "STOCK"
        assert d["source"] == "SIMULATOR"
        assert d["exchange"] == "SIM"
        assert isinstance(d["price"], str) and isinstance(d["volume"], str)
        assert d["volume"].isdigit()
        assert isinstance(d["timestamp"], int) and isinstance(d["receivedAt"], int)
        assert d["receivedAt"] >= d["timestamp"]


def test_empty_or_reversed_interval_yields_nothing():
    generator = _generator()
    assert generator.generate(T0, T0) == []
    assert generator.generate(T0 + 1, T0) == []


def test_poisson_mean_for_small_and_large_lambda():
    rng = random.Random(1)
    for lam in (0.3, 4.0, 200.0):
        samples = [poisson(rng, lam) for _ in range(20_000)]
        assert abs(sum(samples) / len(samples) - lam) < 0.05 * max(lam, 1)
    assert poisson(rng, 0) == 0


def test_to_base36():
    assert to_base36(0) == "0"
    assert to_base36(35) == "z"
    assert to_base36(36) == "10"


# SIM_SOURCE_LABEL: source 값과 tradeId 접두어
def _label_trades(label=None, seconds=2.0):
    kwargs = {} if label is None else {"source_label": label}
    generator = TradeGenerator(
        [Instrument("AAA", 100.0, 0.3, 1.0), Instrument("BBB", 50.0, 0.3, 1.0)], 200.0,
        run_id="abc", seed=1, **kwargs,
    )
    trades = generator.generate(1_760_000_000.0, 1_760_000_000.0 + seconds)
    assert trades
    return trades


def test_default_label_keeps_source_and_trade_id_format():
    for trades in (_label_trades(), _label_trades("SIMULATOR")):
        assert {t.source for t in trades} == {"SIMULATOR"}
        assert all(re.fullmatch(r"SIM-(AAA|BBB)-abc-\d+", t.trade_id) for t in trades)


def test_custom_label_applies_to_source_and_trade_id_prefix_but_not_exchange_or_market_type():
    trades = _label_trades("SIMLOAD")
    assert {t.source for t in trades} == {"SIMLOAD"}
    assert {t.exchange for t in trades} == {"SIM"} and {t.market_type for t in trades} == {"STOCK"}
    assert all(re.fullmatch(r"SIMLOAD-(AAA|BBB)-abc-\d+", t.trade_id) for t in trades)
    d = trades[0].to_dict()
    assert d["source"] == "SIMLOAD" and d["exchange"] == "SIM" and d["marketType"] == "STOCK"


def test_trade_id_and_source_fit_db_columns_with_longest_label():
    """tradeId VARCHAR(128), source VARCHAR(64): 32자 라벨 + 번들 universe 최장 종목 + 먼 미래 run_id + 큰 seq 의 최악 길이"""
    universe = load_universe(UNIVERSE_CSV)
    longest = max((i.symbol for i in universe), key=len)
    label = "X" * 32
    run_id = to_base36(int(4_102_444_800 * 1000)) + "zzzz"  # 서기 2100년 ms + 무작위 접미사 4자
    worst = f"{label}-{longest}-{run_id}-{10 ** 15}"  # seq 16자리 = 사실상 도달 불가능한 상한
    assert len(worst) <= 128, len(worst)
    assert len(label) <= 64

    trades = TradeGenerator(
        [Instrument(longest, 100.0, 0.3, 1.0)], 100.0, run_id=run_id, seed=1, source_label=label,
    ).generate(1_760_000_000.0, 1_760_000_001.0)
    assert trades and all(len(t.trade_id) <= 128 and len(t.source) <= 64 for t in trades)
