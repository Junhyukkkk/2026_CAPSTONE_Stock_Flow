"""미국 정규장(동부시간 월~금 09:30~16:00) 판정. 휴장일은 다루지 않는다."""
from datetime import datetime, time
from functools import lru_cache
from zoneinfo import ZoneInfo

MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)


@lru_cache(maxsize=1)
def _eastern() -> ZoneInfo:
    # tzdata 가 없는 이미지면 여기서 ZoneInfoNotFoundError — 기동 시 호출해 빠르게 실패시킨다
    return ZoneInfo('America/New_York')


def is_us_market_open(epoch_seconds: float) -> bool:
    now = datetime.fromtimestamp(epoch_seconds, _eastern())
    return now.weekday() < 5 and MARKET_OPEN <= now.time() < MARKET_CLOSE
