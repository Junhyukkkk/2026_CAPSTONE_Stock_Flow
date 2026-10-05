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
