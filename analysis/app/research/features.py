"""특징(feature) · 정답(label) 생성.

원칙: t 일의 특징은 t 일 종가까지의 데이터만 쓴다(rolling · shift(+k) 만 사용, shift(-k) 는 정답에서만).
종목 특징은 날짜별 대상 종목 안에서 순위(-0.5 ~ 0.5)로 바꿔 시장 전체 흐름과 분리한다.
시장 특징(동일 비중 지수 수익률, 상승 종목 비율 등)은 그날 모든 종목에 같은 값이다.
"""
import numpy as np
import pandas as pd

from .data import Panel

HORIZON = 5   # 예측 대상: 다음 5일 수익률 (매매도 5일마다)


def _rsi(close: pd.DataFrame, n: int = 14) -> pd.DataFrame:
    diff = close.diff()
    up = diff.clip(lower=0).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    down = (-diff.clip(upper=0)).ewm(alpha=1 / n, adjust=False, min_periods=n).mean()
    return 100 - 100 / (1 + up / down.replace(0, np.nan))


def coin_features(panel: Panel) -> dict:
    """종목별 원시 특징(날짜 × 종목 행렬). 순위 변환 전."""
    c, h, l, v = panel.close, panel.high, panel.low, panel.volume
    ret1 = c.pct_change(fill_method=None)
    f = {}
    for k in (1, 3, 7, 14, 30, 60):
        f[f"ret_{k}"] = c / c.shift(k) - 1
    for n in (5, 20, 50, 200):
        f[f"ma_dist_{n}"] = c / c.rolling(n, min_periods=int(n * 0.8)).mean() - 1
    f["vol_20"] = ret1.rolling(20, min_periods=15).std()
    f["vol_ewma"] = np.sqrt((ret1 ** 2).ewm(alpha=0.06, adjust=False, min_periods=20).mean())
    park = np.log(h / l) ** 2 / (4 * np.log(2))
    f["vol_range"] = np.sqrt(park.ewm(alpha=0.1, adjust=False, min_periods=20).mean())
    f["volume_surge"] = v.rolling(7, min_periods=5).mean() / v.rolling(30, min_periods=20).mean()
    f["quote_volume_30"] = (c * v).rolling(30, min_periods=20).median()
    f["rsi_14"] = _rsi(c)
    f["drawdown_30"] = c / c.rolling(30, min_periods=20).max() - 1
    return f


def market_features(panel: Panel, universe: pd.DataFrame) -> pd.DataFrame:
    """날짜별 시장 특징. 동일 비중 지수는 전날 대상 종목의 오늘 수익률 평균."""
    c = panel.close
    ret1 = c.pct_change(fill_method=None)
    ew = ret1.where(universe.shift(1)).mean(axis=1).fillna(0)
    index = (1 + ew).cumprod()
    above20 = (c > c.rolling(20, min_periods=16).mean()).where(universe)
    btc = c["BTCUSDT"] if "BTCUSDT" in c else index
    m = pd.DataFrame({
        "mkt_ret_7": index / index.shift(7) - 1,
        "mkt_ret_30": index / index.shift(30) - 1,
        "mkt_ma_dist_20": index / index.rolling(20).mean() - 1,
        "mkt_ma_dist_50": index / index.rolling(50).mean() - 1,
        "mkt_vol_20": ew.rolling(20).std(),
        "mkt_breadth_20": above20.mean(axis=1),
        "btc_ret_7": btc / btc.shift(7) - 1,
        "btc_ret_30": btc / btc.shift(30) - 1,
        "btc_ma_dist_20": btc / btc.rolling(20).mean() - 1,
    })
    return m


def labels(panel: Panel, horizon: int = HORIZON) -> pd.DataFrame:
    """t 종가에 사서 t+horizon 종가에 판다고 볼 때의 수익률. 정답에서만 미래 데이터를 쓴다."""
    c = panel.close
    return c.shift(-horizon) / c - 1


def build_dataset(panel: Panel, universe: pd.DataFrame, horizon: int = HORIZON) -> pd.DataFrame:
    """(date, symbol) 긴 형식 표: 종목 특징 순위 + 시장 특징 + 정답(fwd, fwd_rank)."""
    raw = coin_features(panel)
    mkt = market_features(panel, universe)
    fwd = labels(panel, horizon)
    cols = {}
    for name, frame in raw.items():
        cols[name] = frame.where(universe).rank(axis=1, pct=True) - 0.5
    cols["fwd"] = fwd.where(universe)
    cols["fwd_rank"] = cols["fwd"].rank(axis=1, pct=True) - 0.5
    long = pd.concat({k: v.stack(future_stack=True) for k, v in cols.items()}, axis=1)
    long.index.names = ["date", "symbol"]
    long = long[universe.stack(future_stack=True).reindex(long.index).fillna(False).astype(bool)]
    long = long.join(mkt, on="date")
    return long


COIN_FEATURES = ["ret_1", "ret_3", "ret_7", "ret_14", "ret_30", "ret_60", "ma_dist_5", "ma_dist_20",
                 "ma_dist_50", "ma_dist_200", "vol_20", "vol_ewma", "vol_range", "volume_surge",
                 "quote_volume_30", "rsi_14", "drawdown_30"]
MARKET_FEATURES = ["mkt_ret_7", "mkt_ret_30", "mkt_ma_dist_20", "mkt_ma_dist_50", "mkt_vol_20",
                   "mkt_breadth_20", "btc_ret_7", "btc_ret_30", "btc_ma_dist_20"]
