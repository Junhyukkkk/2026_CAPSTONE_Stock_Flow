"""시점별(point-in-time) 투자 대상: 그날까지의 정보만으로 거래대금 상위 N 종목을 고른다."""
import pandas as pd

from .data import Panel

# 가격이 고정되거나 다른 자산을 그대로 따라가는 종목(market-data-sync 의 NON_COIN 과 같은 기준)
NON_COIN = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT", "PAXUSDT",
            "USDSUSDT", "USDSBUSDT", "SUSDUSDT", "USTUSDT", "USDEUSDT", "USD1USDT", "XUSDUSDT",
            "RLUSDUSDT", "BFUSDUSDT", "AEURUSDT", "EURIUSDT", "EURUSDT", "GBPUSDT", "AUDUSDT",
            "BRLUSDT", "TRYUSDT", "RUBUSDT", "UAHUSDT", "BIDRUSDT", "IDRTUSDT", "NGNUSDT",
            "ZARUSDT", "ARSUSDT", "BKRWUSDT", "PAXGUSDT", "XAUTUSDT", "WBTCUSDT", "WBETHUSDT",
            "BETHUSDT", "BNSOLUSDT"}


def point_in_time_universe(panel: Panel, top_n: int = 100, min_history: int = 60,
                           liquidity_window: int = 30) -> pd.DataFrame:
    """날짜 × 종목 bool 행렬. t 행은 t 종가까지의 데이터만 사용한다.

    - 상장 후 min_history 일 이상 거래된 종목
    - 최근 liquidity_window 일 거래대금 중앙값 상위 top_n
    - 스테이블 · 래핑 등 제외, 가격이 거의 안 움직이는 종목(중앙 일간 변동 0.1% 미만) 제외
    """
    close = panel.close
    ret = close.pct_change(fill_method=None)
    keep_cols = [c for c in close.columns if c not in NON_COIN]
    history = close[keep_cols].notna().cumsum()
    liq = panel.quote_volume[keep_cols].rolling(liquidity_window, min_periods=20).median()
    stable_like = ret[keep_cols].abs().rolling(30, min_periods=20).median() < 0.001
    eligible = (history >= min_history) & liq.notna() & ~stable_like & close[keep_cols].notna()
    rank = liq.where(eligible).rank(axis=1, ascending=False)
    universe = (rank <= top_n).reindex(columns=close.columns, fill_value=False)
    return universe.fillna(False)
