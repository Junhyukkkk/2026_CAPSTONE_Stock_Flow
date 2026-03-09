"""
유틸리티 함수 모듈
"""
import asyncio
import logging
import time
from typing import Callable, Any

logger = logging.getLogger(__name__)


class ExponentialBackoff:
    """지수 백오프 재시도 로직"""
    
    def __init__(
        self,
        initial_delay: float = 5.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0
    ):
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.current_delay = initial_delay
    
    def wait(self):
        """현재 지연 시간만큼 동기 대기 (동기 함수용)"""
        time.sleep(self.current_delay)
        # 다음 지연 시간 계산 (최대값 제한)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
    
    async def async_wait(self):
        """현재 지연 시간만큼 비동기 대기 (async 함수용)"""
        await asyncio.sleep(self.current_delay)
        # 다음 지연 시간 계산 (최대값 제한)
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
    
    def reset(self):
        """지연 시간 초기화"""
        self.current_delay = self.initial_delay


def retry_with_backoff(
    func: Callable,
    max_retries: int = -1,  # -1이면 무한 재시도
    initial_delay: float = 5.0,
    max_delay: float = 60.0,
    backoff_multiplier: float = 2.0,
    on_retry: Callable[[Exception, int], None] = None
) -> Any:
    """
    지수 백오프를 사용한 재시도 데코레이터
    
    Args:
        func: 실행할 함수
        max_retries: 최대 재시도 횟수 (-1이면 무한)
        initial_delay: 초기 지연 시간 (초)
        max_delay: 최대 지연 시간 (초)
        backoff_multiplier: 백오프 배수
        on_retry: 재시도 시 호출할 콜백 함수
    
    Returns:
        함수 실행 결과
    """
    backoff = ExponentialBackoff(initial_delay, max_delay, backoff_multiplier)
    retry_count = 0
    
    while True:
        try:
            return func()
        except Exception as e:
            retry_count += 1
            
            if max_retries > 0 and retry_count > max_retries:
                logger.error(f"❌ 최대 재시도 횟수({max_retries}) 초과. 종료합니다.")
                raise
            
            if on_retry:
                on_retry(e, retry_count)
            else:
                logger.warning(f"⚠️ 오류 발생 (재시도 {retry_count}): {e}")
            
            backoff.wait()
