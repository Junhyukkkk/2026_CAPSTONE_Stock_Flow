"""거래소 과거 캔들 초기 백필 (수동 실행).

    # 코인 일봉: 거래 중인 USDT 전 종목 + DB 에 이미 있는 종목, 상장일(또는 --since)부터
    python -m app.backfill daily --symbols ALL --since 2017-01-01

    # 코인 1분봉: 월별 아카이브, 이번 달은 어제까지 일별 아카이브
    python -m app.backfill minute --symbols BTCUSDT --from 2022-01
"""
import argparse
import logging
import time
from datetime import date, datetime, timedelta, timezone

from sqlalchemy import text

from . import binance, store
from .db import get_engine

log = logging.getLogger(__name__)
SOURCE = "BINANCE"


# 가격이 고정되거나 다른 자산을 그대로 따라가는 종목은 거래대금 순위에서 뺀다.
NON_MARKET = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT", "EURUSDT",
              "AEURUSDT", "USD1USDT", "XUSDUSDT", "RLUSDUSDT", "USDEUSDT", "BFUSDUSDT", "EURIUSDT",
              "PAXGUSDT", "XAUTUSDT", "WBTCUSDT", "WBETHUSDT", "BNSOLUSDT", "USDSUSDT"}


def top_by_quote_volume(n: int, days: int = 30) -> list:
    """일봉 기준 최근 N일 거래대금(close*volume) 상위 종목. 거래 중인 종목만."""
    trading = set(binance.usdt_symbols())
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT symbol, sum(close * volume) AS quote_volume
            FROM symbol_daily_ohlcv
            WHERE source = :s AND trade_date >= current_date - :days
            GROUP BY symbol ORDER BY quote_volume DESC"""), {"s": SOURCE, "days": days}).fetchall()
    ranked = [r.symbol for r in rows if r.symbol in trading and r.symbol not in NON_MARKET]
    return ranked[:n]


def resolve_symbols(value: str) -> list:
    """ALL | TOP:<n> | 쉼표로 구분한 심볼 목록.

    ALL 은 Binance 에서 현재 거래 중인 USDT 종목만이다. DB 에는 부하 테스트용 가짜 종목(SYM000 등)이나
    상장폐지 종목도 있는데, REST 로는 받을 수 없다.
    """
    if value.upper() == "ALL":
        return binance.usdt_symbols()
    if value.upper().startswith("TOP:"):
        return top_by_quote_volume(int(value.split(":", 1)[1]))
    return [s.strip().upper() for s in value.split(",") if s.strip()]


def backfill_daily(symbols: list, since: date) -> dict:
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = datetime(since.year, since.month, since.day, tzinfo=timezone.utc)
    summary = {"ok": 0, "failed": [], "rows": 0}
    for i, symbol in enumerate(symbols, 1):
        started = time.monotonic()
        try:
            candles = binance.rest_klines(symbol, "1d", start, today)
            rows = store.upsert_daily(symbol, SOURCE, "CRYPTO", candles)
            first, last, count, missing = store.update_coverage(symbol, SOURCE, "1d")
            summary["ok"] += 1
            summary["rows"] += rows
            log.info("[%d/%d] daily %s candles=%d upserted=%d range=%s..%s missing=%s took=%.1fs",
                     i, len(symbols), symbol, len(candles), rows,
                     first and first.date(), last and last.date(), missing, time.monotonic() - started)
        except Exception as e:  # 한 종목 실패가 전체를 멈추지 않게
            summary["failed"].append(symbol)
            log.exception("[%d/%d] daily %s failed: %s", i, len(symbols), symbol, e)
    return summary


def _minute_periods(first_month: str, last_month: str = None) -> list:
    """완료된 달은 월별 파일, 이번 달은 1일~어제 일별 파일."""
    today = datetime.now(timezone.utc).date()
    first = date.fromisoformat(first_month + "-01")
    last_complete = today.replace(day=1) - timedelta(days=1)
    last = date.fromisoformat(last_month + "-01") if last_month else last_complete
    periods = binance.month_periods(first, min(last, last_complete))
    if last_month is None:
        day = today.replace(day=1)
        while day < today:
            periods.append(day.isoformat())
            day += timedelta(days=1)
    return periods


def backfill_minutes(symbols: list, first_month: str, last_month: str = None) -> dict:
    periods = _minute_periods(first_month, last_month)
    summary = {"ok": 0, "failed": [], "rows": 0, "files": 0, "missing_files": 0}
    for i, symbol in enumerate(symbols, 1):
        started = time.monotonic()
        store.update_coverage(symbol, SOURCE, "1m", status="BACKFILLING")
        try:
            for period in periods:
                t0 = time.monotonic()
                candles = binance.download_archive(symbol, "1m", period)
                if candles is None:
                    summary["missing_files"] += 1
                    log.debug("%s %s: no archive (not listed yet?)", symbol, period)
                    continue
                rows = store.upsert_minutes(symbol, SOURCE, candles)
                summary["files"] += 1
                summary["rows"] += rows
                log.info("[%d/%d] 1m %s %s candles=%d upserted=%d took=%.1fs",
                         i, len(symbols), symbol, period, len(candles), rows, time.monotonic() - t0)
            first, last, count, missing = store.update_coverage(symbol, SOURCE, "1m")
            summary["ok"] += 1
            log.info("[%d/%d] 1m %s done rows=%d range=%s..%s missing=%s took=%.0fs",
                     i, len(symbols), symbol, count, first, last, missing, time.monotonic() - started)
        except Exception as e:
            summary["failed"].append(symbol)
            store.update_coverage(symbol, SOURCE, "1m", note=f"backfill failed: {e}"[:500])
            log.exception("[%d/%d] 1m %s failed: %s", i, len(symbols), symbol, e)
    return summary


def main():
    parser = argparse.ArgumentParser(description="거래소 과거 캔들 초기 백필")
    sub = parser.add_subparsers(dest="kind", required=True)
    d = sub.add_parser("daily")
    d.add_argument("--symbols", default="ALL")
    d.add_argument("--since", type=date.fromisoformat, default=date(2017, 1, 1))
    m = sub.add_parser("minute")
    m.add_argument("--symbols", required=True)
    m.add_argument("--from", dest="first_month", required=True, help="YYYY-MM")
    m.add_argument("--to", dest="last_month", help="YYYY-MM (생략 시 어제까지)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    started = time.monotonic()
    symbols = resolve_symbols(args.symbols)
    log.info("%s backfill: %d symbols", args.kind, len(symbols))
    if args.kind == "daily":
        summary = backfill_daily(symbols, args.since)
    else:
        summary = backfill_minutes(symbols, args.first_month, args.last_month)
    log.info("backfill done in %.0fs: %s", time.monotonic() - started, summary)


if __name__ == "__main__":
    main()
