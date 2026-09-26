"""시계열 전처리: 결측치 처리 / 이상치 제거 / 균일 주기 정렬.

ARIMA 는 균일한 간격의 결측 없는 시계열을 가정하므로,
- 중복 타임스탬프 제거
- 주기 재색인(asfreq)으로 비어있는 구간을 NaN 으로 노출
- 롤링 중앙값 기반 이상치 탐지 후 NaN 처리
- 시간 보간 + 앞/뒤 채움으로 결측 메움
"""
import numpy as np
import pandas as pd

# interval 코드 → pandas offset alias
FREQ = {"1m": "1min", "1d": "1D"}


def to_close_series(df: pd.DataFrame) -> pd.Series:
    """OHLCV DataFrame → 종가 Series (ts 인덱스)."""
    s = df.set_index("ts")["close"].astype(float)
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def clean_series(
    s: pd.Series,
    freq: str = "1min",
    outlier_z: float = 8.0,
    interpolate: bool = True,
) -> pd.Series:
    """종가 Series 를 정제하여 반환.

    Parameters
    ----------
    freq : pandas offset alias (예: "1min", "1D"). None 이면 재색인 생략.
    outlier_z : 롤링 잔차 표준편차의 몇 배를 이상치로 볼지. 0 이면 비활성화.
    interpolate : 결측 보간 여부.
    """
    s = s.copy()
    s = s[~s.index.duplicated(keep="last")].sort_index()

    if freq:
        s = s.asfreq(freq)

    if outlier_z and s.notna().sum() > 10:
        median = s.rolling(window=20, min_periods=5, center=True).median()
        resid = s - median
        std = resid.std()
        if std and std > 0:
            outliers = resid.abs() > (outlier_z * std)
            s[outliers] = np.nan

    if interpolate:
        s = s.interpolate(method="time").ffill().bfill()

    return s
