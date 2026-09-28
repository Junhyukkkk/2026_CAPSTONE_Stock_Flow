"""백테스트: 매 REBALANCE 일마다 결정, 결정은 그날 종가 기준, 수익은 다음 날부터 반영.

비용은 비중 변화량 × COST (편도 수수료 10bp + 슬리피지 5bp).
"""
import numpy as np
import pandas as pd

COST = 0.0015
REBALANCE = 5


def daily_returns(close: pd.DataFrame) -> pd.DataFrame:
    return close.pct_change(fill_method=None)


def ew_index_returns(close: pd.DataFrame, universe: pd.DataFrame) -> pd.Series:
    """전날 대상 종목의 오늘 수익률 동일 비중 평균(매일 재조정, 비용 제외) — 시장 지수."""
    return daily_returns(close).where(universe.shift(1)).mean(axis=1).fillna(0.0)


def is_rebalance_day(t: pd.Timestamp, start: pd.Timestamp, rebalance: int = REBALANCE) -> bool:
    """모든 참가자가 같은 달력(시작일부터 rebalance 일마다)으로 매매한다.

    점수가 매일 있는 모델과 매매일에만 있는 모델(Chronos)을 공정하게 비교하기 위해서다.
    """
    return (t - start).days % rebalance == 0


def top_k_portfolio(scores: pd.Series, close: pd.DataFrame, k: int = 10, cost: float = COST,
                    rebalance: int = REBALANCE, exposure: pd.Series = None,
                    calendar_start: pd.Timestamp = None) -> pd.Series:
    """점수 상위 k 종목 동일 비중. exposure(0~1, 날짜별)가 있으면 그 비율만 투자."""
    ret = daily_returns(close)
    wide = scores.unstack("symbol")
    start = calendar_start if calendar_start is not None else wide.index[0]
    dates = close.loc[wide.index[0]:wide.index[-1]].index
    w = pd.Series(0.0, index=close.columns)
    out = []
    for i, t in enumerate(dates[:-1]):
        new_w = w
        if is_rebalance_day(t, start, rebalance) and t in wide.index:
            row = wide.loc[t].dropna()
            new_w = pd.Series(0.0, index=close.columns)
            if len(row):
                top = row.nlargest(k).index
                new_w[top] = 1.0 / len(top)
            if exposure is not None:
                new_w = new_w * float(exposure.get(t, 1.0))
        c = (new_w - w).abs().sum() * cost
        nxt = dates[i + 1]
        r = ret.loc[nxt].reindex(close.columns).fillna(0.0)
        gross = float((new_w * r).sum())
        out.append((nxt, gross - c))
        w = new_w * (1 + r) / (1 + gross) if new_w.sum() else new_w
    return pd.Series(dict(out))


def tranched(strategy, start, tranches: int = REBALANCE) -> pd.Series:
    """자금을 tranches 등분해 각각 다른 날(시작일 + 0..tranches-1 일)에 매매한 결과의 평균.

    5일마다 한 번 매매하면 '어느 날 매매했느냐'라는 운이 결과를 크게 흔든다(개발 중 확인: 매매일을 며칠
    옮기자 같은 모델의 6개월 수익률이 -23% → -35%). strategy(calendar_start) → 일별 수익률.
    """
    start = pd.Timestamp(start)
    runs = [strategy(start + pd.Timedelta(days=off)) for off in range(tranches)]
    return pd.concat(runs, axis=1).fillna(0.0).mean(axis=1)


def equal_weight_portfolio(universe: pd.DataFrame, close: pd.DataFrame, start, end,
                           exclude_top_vol: float = None, vol: pd.DataFrame = None,
                           cost: float = COST, rebalance: int = REBALANCE,
                           calendar_start: pd.Timestamp = None) -> pd.Series:
    """대상 종목 동일 비중(5일마다 재조정). exclude_top_vol=0.1 이면 변동성 상위 10% 제외."""
    u = universe.loc[start:end]
    scores = u.astype(float).where(u)
    if exclude_top_vol:
        vr = vol.loc[start:end].where(u).rank(axis=1, pct=True)
        scores = scores.where(vr <= 1 - exclude_top_vol)
    s = scores.stack()
    cal = calendar_start if calendar_start is not None else pd.Timestamp(start)
    return top_k_portfolio(s, close, k=10**6, cost=cost, rebalance=rebalance, calendar_start=cal)


def timing_strategy(asset_ret: pd.Series, signal: pd.Series, cost: float = COST, calendar_start=None,
                    rebalance: int = REBALANCE) -> pd.Series:
    """signal(0/1 또는 확률 → 0.5 기준) 을 rebalance 일마다 반영해 asset 을 보유/현금."""
    start = calendar_start if calendar_start is not None else signal.index[0]
    dates = asset_ret.loc[signal.index[0]:signal.index[-1]].index
    pos, out = 0.0, []
    for i, t in enumerate(dates[:-1]):
        new_pos = pos
        if is_rebalance_day(t, start, rebalance) and t in signal.index:
            s = signal.loc[t]
            new_pos = float(s > 0.5) if not np.isnan(s) else pos
        c = abs(new_pos - pos) * cost
        nxt = dates[i + 1]
        out.append((nxt, new_pos * float(asset_ret.get(nxt, 0.0)) - c))
        pos = new_pos
    return pd.Series(dict(out))
