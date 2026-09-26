"""결측 복구와 일일 확정: 빠지거나 불완전한 분을 거래소 공식 캔들로 채운다.

- repair_gaps  : 최근 N시간에서 빠진 분만 찾아 REST 로 채움 (매시)
- confirm_day  : 전날 하루치 1분봉 · 일봉을 거래소 공식본으로 덮어씀 (매일)
                 실시간 수집 중 일부만 집계된 분도 여기서 바로잡힌다.

대상은 현재 거래 중인 Binance USDT 종목 중 ohlcv_1m 에 데이터가 있는 종목이다.
fetch · trading 은 테스트에서 외부 호출 없이 바꿔 끼울 수 있다.
"""
import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from . import binance, store
from .config import settings
from .db import get_engine

log = logging.getLogger(__name__)
SOURCE = "BINANCE"
MINUTE = timedelta(minutes=1)

# 종목별 창 안의 봉 수만 센다. 체결이 없는 분은 수집 집계에 봉이 없으므로(거래소는 거래량 0 봉을 준다)
# 빈 분을 하나씩 찾아 따로 요청하면 호출이 폭증한다. 대신 빠진 게 있는 종목은 창 전체를 한 번에 받는다.
# 창 안에 데이터가 전혀 없는 종목(수집이 멈춘 경우)도 잡도록 대상은 '최근 하루 동안 보인 종목'이다.
_PRESENT_SQL = text("""
    SELECT symbol, count(*) FILTER (WHERE bucket >= :from_ts) AS present
    FROM ohlcv_1m
    WHERE source = :src AND bucket >= :seen_from AND bucket < :to_ts
    GROUP BY symbol
""")


def count_present(from_ts: datetime, to_ts: datetime) -> dict:
    with get_engine().connect() as conn:
        rows = conn.execute(_PRESENT_SQL, {"src": SOURCE, "from_ts": from_ts, "to_ts": to_ts,
                                           "seen_from": from_ts - timedelta(days=1)}).fetchall()
    return {symbol: present for symbol, present in rows}


def repair_gaps(now: datetime = None, fetch=None, trading=None, hours: int = None) -> dict:
    fetch = fetch or binance.rest_klines
    now = now or datetime.now(timezone.utc)
    to_ts = now.replace(second=0, microsecond=0) - timedelta(minutes=settings.repair_settle_minutes)
    from_ts = to_ts - timedelta(hours=hours or settings.repair_lookback_hours)
    expected = int((to_ts - from_ts) / MINUTE)
    started = time.monotonic()
    present = count_present(from_ts, to_ts)
    trading = trading if trading is not None else set(binance.usdt_symbols())
    summary = {"symbols_checked": 0, "symbols_with_gaps": 0, "missing_minutes": 0,
               "filled": 0, "unfillable": 0, "failed": []}
    for symbol, have in sorted(present.items()):
        if symbol not in trading:  # 상장폐지·거래중지 종목은 채울 수 없다
            continue
        summary["symbols_checked"] += 1
        if have >= expected:
            continue
        summary["symbols_with_gaps"] += 1
        summary["missing_minutes"] += expected - have
        try:
            candles = fetch(symbol, "1m", from_ts, to_ts)
            summary["filled"] += store.upsert_minutes(symbol, SOURCE, candles)
            # 창 중간 상장 · 거래소 중단 구간은 거래소에도 봉이 없다.
            summary["unfillable"] += max(0, expected - len(candles))
        except Exception:
            summary["failed"].append(symbol)
            log.exception("repair %s failed", symbol)
    log.info("repair %s..%s %s took=%.1fs", from_ts.isoformat(), to_ts.isoformat(), summary,
             time.monotonic() - started)
    return summary


def _symbols_on(day: date) -> list:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    with get_engine().connect() as conn:
        return [r[0] for r in conn.execute(text("""
            SELECT DISTINCT symbol FROM ohlcv_1m
            WHERE source = :src AND bucket >= :a AND bucket < :b"""),
            {"src": SOURCE, "a": start, "b": start + timedelta(days=1)})]


def confirm_day(day: date = None, fetch=None, trading=None, refresh_coverage: bool = True) -> dict:
    """day(기본: 어제 UTC)의 1분봉과 일봉을 거래소 공식본으로 확정한다."""
    fetch = fetch or binance.rest_klines
    day = day or (datetime.now(timezone.utc).date() - timedelta(days=1))
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    trading = trading if trading is not None else set(binance.usdt_symbols())
    symbols = sorted(set(_symbols_on(day)) & trading)
    started = time.monotonic()
    summary = {"day": day.isoformat(), "symbols": len(symbols), "minutes_changed": 0,
               "daily_rows": 0, "failed": []}
    for symbol in symbols:
        try:
            summary["minutes_changed"] += store.upsert_minutes(
                symbol, SOURCE, fetch(symbol, "1m", start, end))
            summary["daily_rows"] += store.upsert_daily(
                symbol, SOURCE, "CRYPTO", fetch(symbol, "1d", start, end))
            if refresh_coverage:
                store.update_coverage(symbol, SOURCE, "1d")
        except Exception:
            summary["failed"].append(symbol)
            log.exception("confirm %s %s failed", day, symbol)
    log.info("confirm %s took=%.1fs", summary, time.monotonic() - started)
    return summary


def main():
    parser = argparse.ArgumentParser(description="결측 복구 / 일일 확정 수동 실행")
    sub = parser.add_subparsers(dest="kind", required=True)
    r = sub.add_parser("repair")
    r.add_argument("--hours", type=int, help="검사 범위(시간). 서버가 오래 멈췄던 뒤에는 크게 준다")
    c = sub.add_parser("confirm")
    c.add_argument("--day", type=date.fromisoformat, help="YYYY-MM-DD (기본: 어제 UTC)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    if args.kind == "repair":
        repair_gaps(hours=args.hours)
    else:
        confirm_day(args.day)


if __name__ == "__main__":
    main()
