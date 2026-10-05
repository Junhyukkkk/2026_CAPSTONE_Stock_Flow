"""미국 정규장(동부시간 월~금 09:30~16:00) 판정. 휴장일은 다루지 않는다."""
from datetime import datetime, time
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


@lru_cache(maxsize=1)
def _eastern() -> ZoneInfo:
    # 기동 시 한 번 호출해 tz 데이터베이스 누락을 첫 틱이 아니라 시작 시점에 알린다
    try:
        return ZoneInfo('America/New_York')
    except ZoneInfoNotFoundError as e:
        raise RuntimeError(
            "시간대 데이터베이스(tzdata)를 찾을 수 없어 SIM_MARKET_HOURS=us 를 사용할 수 없습니다. "
            "`pip install tzdata` 로 설치하거나 SIM_MARKET_HOURS=always 로 실행하세요"
        ) from e


def is_us_market_open(epoch_seconds: float) -> bool:
    now = datetime.fromtimestamp(epoch_seconds, _eastern())
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE


# 정규장 장중 거래 강도 곡선(U자): (개장 후 분, 상대 강도) 꼭짓점 사이를 선형 보간하고,
# 장 전체 평균이 정확히 1.0 이 되도록 정규화한다(구간별 선형이라 사다리꼴 합이 곧 적분값).
SESSION_MINUTES = 390.0
_PROFILE_KNOTS = ((0.0, 3.0), (30.0, 1.5), (120.0, 0.7), (150.0, 0.6), (270.0, 0.7), (360.0, 1.2), (390.0, 2.5))
_PROFILE_NORM = sum(
    (m1 - m0) * (v0 + v1) / 2 for (m0, v0), (m1, v1) in zip(_PROFILE_KNOTS, _PROFILE_KNOTS[1:])
) / SESSION_MINUTES


def intraday_profile(minutes_since_open: float) -> float:
    """개장 후 경과 분 → 상대 강도(장중 평균 1.0). [0, 390) 밖은 0."""
    if not 0.0 <= minutes_since_open < SESSION_MINUTES:
        return 0.0
    for (m0, v0), (m1, v1) in zip(_PROFILE_KNOTS, _PROFILE_KNOTS[1:]):
        if minutes_since_open < m1:
            return (v0 + (v1 - v0) * (minutes_since_open - m0) / (m1 - m0)) / _PROFILE_NORM
    return 0.0  # 도달 불가


def us_intraday_profile(epoch_seconds: float) -> float:
    """미국 정규장 중이면 그 시각의 상대 강도, 장외·주말이면 0."""
    now = datetime.fromtimestamp(epoch_seconds, _eastern())
    if now.weekday() >= 5:
        return 0.0
    minutes = (now.hour * 3600 + now.minute * 60 + now.second + now.microsecond / 1e6) / 60.0 - 9.5 * 60
    return intraday_profile(minutes)
