"""평가 지표: 예측력(IC, 적중률) · 전략 성과(수익, 샤프, 낙폭) · 연도별 분해 · 부트스트랩 신뢰구간."""
import numpy as np
import pandas as pd


def perf(daily: pd.Series) -> dict:
    daily = daily.dropna()
    if daily.empty:
        return {}
    eq = (1 + daily).cumprod()
    years = len(daily) / 365
    sd = daily.std()
    return {
        "total_ret_pct": (eq.iloc[-1] - 1) * 100,
        "cagr_pct": (eq.iloc[-1] ** (1 / years) - 1) * 100 if years > 0 and eq.iloc[-1] > 0 else np.nan,
        "sharpe": daily.mean() / sd * np.sqrt(365) if sd > 0 else 0.0,
        "mdd_pct": ((eq.cummax() - eq) / eq.cummax()).max() * 100,
        "days": len(daily),
    }


def yearly_returns(daily: pd.Series) -> dict:
    return {int(y): ((1 + g).prod() - 1) * 100 for y, g in daily.groupby(daily.index.year)}


def rank_ic(scores: pd.Series, fwd: pd.Series, horizon: int = 5) -> dict:
    """날짜별 스피어만 상관. t-통계는 겹치지 않는 날짜(horizon 간격)로 계산."""
    df = pd.DataFrame({"s": scores, "f": fwd}).dropna()
    ics = df.groupby(level="date").apply(
        lambda g: g["s"].rank().corr(g["f"].rank()) if len(g) >= 30 else np.nan).dropna()
    if ics.empty:
        return {}
    step = ics.iloc[::horizon]
    return {"ic_mean": ics.mean(), "ic_t": step.mean() / step.std() * np.sqrt(len(step)),
            "ic_pos_pct": (ics > 0).mean() * 100}


def sharpe_diff_ci(a: pd.Series, b: pd.Series, block: int = 20, n: int = 1000, seed: int = 0) -> dict:
    """전략 a 와 기준 b 의 샤프 차이 블록 부트스트랩 95% 구간(날짜를 맞춰 같이 리샘플)."""
    df = pd.DataFrame({"a": a, "b": b}).dropna()
    x, y = df["a"].to_numpy(), df["b"].to_numpy()
    T = len(x)
    if T < block * 5:
        return {}
    rng = np.random.default_rng(seed)
    sh = lambda r: r.mean() / r.std() * np.sqrt(365) if r.std() > 0 else 0.0
    diffs = []
    for _ in range(n):
        starts = rng.integers(0, T - block, size=T // block + 1)
        idx = np.concatenate([np.arange(s, s + block) for s in starts])[:T]
        diffs.append(sh(x[idx]) - sh(y[idx]))
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return {"sharpe_diff": sh(x) - sh(y), "ci_lo": lo, "ci_hi": hi, "p_better": float(np.mean(np.array(diffs) > 0))}
