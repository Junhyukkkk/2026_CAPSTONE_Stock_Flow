"""모델 경쟁(개발 구간) 실행.

    python -m app.research.run --eval-start 2024-01-01 --eval-end 2026-07-31 --out /out
    python -m app.research.run --holdout --models LGBM,LINEAR --chronos small --out /out   # 최종 1회 시험

봉인 구간은 data.load_panel 이 막는다. --holdout 은 PREREGISTRATION.md 에 적은 후보 · 기준으로 단 한 번만
실행한다. 결과는 표로 출력하고 <out>/arena_<구간>.json, 누적 수익 곡선은 curves_<구간>.csv 에 저장한다.
"""
import argparse
import json
import logging
import time
from datetime import date
from pathlib import Path

import pandas as pd

from . import backtest as bt
from . import chronos
from . import metrics
from .data import HOLDOUT_END, HOLDOUT_START, load_panel
from .features import HORIZON, build_dataset, coin_features
from .models import (FEATURES_A, RULES_A, RULES_B, market_dataset, rule_scores, walk_forward,
                     walk_forward_market)
from .split import monthly_folds
from .universe import point_in_time_universe

log = logging.getLogger("arena")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--data-start", default="2019-01-01")
    p.add_argument("--eval-start", default="2024-01-01")
    p.add_argument("--eval-end", default="2026-07-31")
    p.add_argument("--top-n", type=int, default=100)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--models", default="LINEAR,LGBM")
    p.add_argument("--chronos", default="tiny,small", help="빈 문자열이면 Chronos 제외")
    p.add_argument("--out", default="/out")
    p.add_argument("--holdout", action="store_true", help="사전 등록한 봉인 구간 최종 시험(1회)")
    a = p.parse_args()
    if a.holdout:
        a.eval_start, a.eval_end = HOLDOUT_START.isoformat(), HOLDOUT_END.isoformat()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    t0 = time.monotonic()

    panel = load_panel(start=date.fromisoformat(a.data_start), end=date.fromisoformat(a.eval_end),
                       allow_holdout=a.holdout)
    universe = point_in_time_universe(panel, top_n=a.top_n)
    ds = build_dataset(panel, universe)
    log.info("panel %s..%s symbols=%d rows=%d (%.0fs)", panel.close.index[0].date(),
             panel.close.index[-1].date(), panel.close.shape[1], len(ds), time.monotonic() - t0)
    folds = monthly_folds(a.eval_start, a.eval_end)
    start, end = pd.Timestamp(a.eval_start), pd.Timestamp(a.eval_end)
    close = panel.close
    fwd = ds["fwd"]

    # ------------------------------------------------------------ 과제 A: 종목 선택
    scores = {name: rule_scores(ds, name, start, end) for name in RULES_A}
    for m in a.models.split(","):
        t = time.monotonic()
        scores[m] = walk_forward(ds, folds, m, "fwd_rank", FEATURES_A)
        log.info("A %s trained (%.0fs)", m, time.monotonic() - t)
    for size in filter(None, a.chronos.split(",")):
        t = time.monotonic()
        scores[f"CHRONOS_{size.upper()}"] = chronos.coin_scores(close, universe, start, end, size)
        log.info("A chronos-%s predicted (%.0fs)", size, time.monotonic() - t)
    # 모든 전략은 5개 트랜치(서로 다른 날 매매)의 평균으로 평가한다(매매일 운 제거).
    vol20 = coin_features(panel)["vol_20"]
    bench = bt.tranched(lambda cs: bt.equal_weight_portfolio(universe, close, start, end, calendar_start=cs), start)
    curves = {"A:EW_UNIVERSE": bench}
    results_a = {"EW_UNIVERSE (benchmark)": {"perf": metrics.perf(bench), "yearly": metrics.yearly_returns(bench)}}
    exhv = bt.tranched(lambda cs: bt.equal_weight_portfolio(universe, close, start, end, exclude_top_vol=0.1,
                                                            vol=vol20, calendar_start=cs), start)
    curves["A:EW_EX_HIGHVOL"] = exhv
    results_a["EW_EX_HIGHVOL"] = {"perf": metrics.perf(exhv), "yearly": metrics.yearly_returns(exhv),
                                  "vs_bench": metrics.sharpe_diff_ci(exhv, bench)}
    for name, s in scores.items():
        port = bt.tranched(lambda cs: bt.top_k_portfolio(s, close, k=a.k, calendar_start=cs), start)
        curves[f"A:TOP{a.k}_{name}"] = port
        results_a[f"TOP{a.k}_{name}"] = {
            "ic": metrics.rank_ic(s, fwd.reindex(s.index), HORIZON),
            "perf": metrics.perf(port), "yearly": metrics.yearly_returns(port),
            "vs_bench": metrics.sharpe_diff_ci(port, bench)}

    # ------------------------------------------------------------ 과제 B: 시장 진입
    idx_ret = bt.ew_index_returns(close, universe)
    idx_level = (1 + idx_ret).cumprod()
    idx_fwd = idx_level.shift(-HORIZON) / idx_level - 1
    mkt = market_dataset(ds, idx_fwd)
    mkt_eval = mkt.loc[start:end]
    btc_ret = close["BTCUSDT"].pct_change(fill_method=None)
    signals = {name: rule(mkt_eval) for name, rule in RULES_B.items()}
    for m in ("LOGIT", "LGBM"):
        signals[f"MODEL_{m}"] = walk_forward_market(mkt, folds, m)
    for size in filter(None, a.chronos.split(",")):
        signals[f"MODEL_CHRONOS_{size.upper()}"] = chronos.market_signal(idx_level, start, end, size)
    results_b = {}
    def timing(r, sig):
        return bt.tranched(lambda cs: bt.timing_strategy(r, sig, calendar_start=cs), start)

    base = {asset: timing(r, signals["ALWAYS_IN"]) for asset, r in (("EW_INDEX", idx_ret), ("BTC", btc_ret))}
    for name, sig in signals.items():
        hit = None
        if name.startswith("MODEL_"):
            j = pd.DataFrame({"p": sig, "up": mkt["up"]}).dropna()
            hit = float(((j["p"] > 0.5) == (j["up"] > 0.5)).mean() * 100)
        for asset, r in (("EW_INDEX", idx_ret), ("BTC", btc_ret)):
            strat = timing(r, sig)
            curves[f"B:{asset}:{name}"] = strat
            exposure = float((sig > 0.5).mean() * 100)
            results_b[f"{asset}:{name}"] = {"perf": metrics.perf(strat), "yearly": metrics.yearly_returns(strat),
                                            "exposure_pct": exposure, "hit_pct": hit,
                                            "vs_always_in": metrics.sharpe_diff_ci(strat, base[asset])}

    out = {"eval": [a.eval_start, a.eval_end], "holdout": a.holdout, "top_n": a.top_n, "k": a.k,
           "trials": len(results_a) + len(results_b), "task_a": results_a, "task_b": results_b}
    Path(a.out).mkdir(parents=True, exist_ok=True)
    path = Path(a.out) / f"arena_{a.eval_start}_{a.eval_end}.json"
    path.write_text(json.dumps(out, indent=1, default=float))
    eq = (1 + pd.DataFrame(curves).fillna(0.0)).cumprod()
    eq.to_csv(Path(a.out) / f"curves_{a.eval_start}_{a.eval_end}.csv", float_format="%.6f")
    _print(out)
    log.info("done in %.0fs -> %s", time.monotonic() - t0, path)


def _print(out):
    pd.set_option("display.width", 220)
    rows = []
    for name, r in out["task_a"].items():
        rows.append({"strategy": name, **{k: r["perf"].get(k) for k in ("total_ret_pct", "sharpe", "mdd_pct")},
                     "ic": (r.get("ic") or {}).get("ic_mean"), "ic_t": (r.get("ic") or {}).get("ic_t"),
                     "sharpe_vs_bench": (r.get("vs_bench") or {}).get("sharpe_diff"),
                     "ci_lo": (r.get("vs_bench") or {}).get("ci_lo"), "ci_hi": (r.get("vs_bench") or {}).get("ci_hi"),
                     **{f"y{y}": v for y, v in r["yearly"].items()}})
    print("\n=== TASK A: coin selection (top-k, weekly rebalance, 15bp one-way) ===")
    print(pd.DataFrame(rows).round(3).to_string(index=False))
    rows = []
    for name, r in out["task_b"].items():
        rows.append({"strategy": name, **{k: r["perf"].get(k) for k in ("total_ret_pct", "sharpe", "mdd_pct")},
                     "exposure": r["exposure_pct"], "hit": r["hit_pct"],
                     "sharpe_vs_in": (r.get("vs_always_in") or {}).get("sharpe_diff"),
                     "ci_lo": (r.get("vs_always_in") or {}).get("ci_lo"),
                     "ci_hi": (r.get("vs_always_in") or {}).get("ci_hi"),
                     **{f"y{y}": v for y, v in r["yearly"].items()}})
    print("\n=== TASK B: market timing (decide every 5 days) ===")
    print(pd.DataFrame(rows).round(3).to_string(index=False))


if __name__ == "__main__":
    main()
