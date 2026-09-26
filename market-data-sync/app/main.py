"""market-data-sync 진입점: 주기 작업 스케줄러.

- 실시간 동기화: 1분마다 최근 구간, 매시 7분에 넓은 구간 (market_ticks_1m → ohlcv_1m LIVE)
- 결측 복구: 매시 20분, 최근 48시간의 빈 분을 거래소 REST 로 채움
- 일일 확정: 매일 01:30 UTC, 전날 1분봉 · 일봉을 거래소 공식본으로 덮어씀
"""
import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from . import live_sync, repair
from .config import settings

log = logging.getLogger("market-data-sync")


def _safe(job, *args):
    """작업 하나가 실패해도 스케줄러는 계속 돈다."""
    try:
        job(*args)
    except Exception:
        log.exception("job failed: %s", getattr(job, "__name__", job))


def build_scheduler() -> BlockingScheduler:
    scheduler = BlockingScheduler(timezone="UTC",
                                  job_defaults={"coalesce": True, "max_instances": 1})
    scheduler.add_job(_safe, "interval", seconds=settings.live_sync_interval_seconds,
                      args=[live_sync.run_once, settings.live_sync_lookback_minutes,
                            settings.live_sync_settle_minutes],
                      id="live_sync")
    scheduler.add_job(_safe, "cron", minute=7,
                      args=[live_sync.run_once, settings.live_sync_wide_lookback_minutes,
                            settings.live_sync_settle_minutes],
                      id="live_sync_wide")
    scheduler.add_job(_safe, "cron", minute=20, args=[repair.repair_gaps], id="repair_gaps")
    scheduler.add_job(_safe, "cron", hour=settings.confirm_hour_utc,
                      minute=settings.confirm_minute_utc, args=[repair.confirm_day],
                      id="confirm_day", misfire_grace_time=3600)
    return scheduler


def main():
    logging.basicConfig(level=settings.log_level,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    # 기동 직후 넓은 구간을 한 번 맞춰 두고 주기 작업을 시작한다.
    _safe(live_sync.run_once, settings.live_sync_wide_lookback_minutes,
          settings.live_sync_settle_minutes)
    build_scheduler().start()


if __name__ == "__main__":
    main()
