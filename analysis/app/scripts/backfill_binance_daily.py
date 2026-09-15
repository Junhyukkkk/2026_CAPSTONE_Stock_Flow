"""Backfill missing Binance daily candles without overwriting existing rows."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from sqlalchemy import text

from app.db import get_engine


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
SOURCE = "BINANCE"
UTC = timezone.utc

EXISTING_DATES_SQL = text(
    """
    SELECT trade_date
    FROM symbol_daily_ohlcv
    WHERE symbol = :symbol
      AND source = :source
      AND trade_date BETWEEN :from_date AND :to_date
    """
)

INSERT_SQL = text(
    """
    INSERT INTO symbol_daily_ohlcv (
        symbol, trade_date, market_type, source,
        open, high, low, close, volume, tick_count, computed_at
    ) VALUES (
        :symbol, :trade_date, :market_type, :source,
        :open, :high, :low, :close, :volume, :tick_count, NOW()
    )
    ON CONFLICT (symbol, trade_date, source) DO NOTHING
    """
)


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"Invalid date '{value}'. Expected YYYY-MM-DD."
        ) from exc


def validate_range(from_date: date, to_date: date, today: date | None = None) -> None:
    current_date = today or datetime.now(UTC).date()
    if from_date > to_date:
        raise ValueError("from_date must be earlier than or equal to to_date")
    if to_date >= current_date:
        raise ValueError(
            "to_date must be before the current UTC date; incomplete daily candles are rejected"
        )


def _utc_millis(day: date, *, end_of_day: bool = False) -> int:
    boundary = time.max if end_of_day else time.min
    return int(datetime.combine(day, boundary, tzinfo=UTC).timestamp() * 1000)


def parse_kline(symbol: str, raw: list[Any]) -> dict[str, Any]:
    if len(raw) < 9:
        raise ValueError("Binance kline response has fewer than 9 fields")

    try:
        open_price = Decimal(str(raw[1]))
        high_price = Decimal(str(raw[2]))
        low_price = Decimal(str(raw[3]))
        close_price = Decimal(str(raw[4]))
        volume = Decimal(str(raw[5]))
        tick_count = int(raw[8])
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Binance kline contains an invalid numeric value") from exc

    prices = (open_price, high_price, low_price, close_price)
    if not all(value.is_finite() and value > 0 for value in prices):
        raise ValueError("OHLC prices must be finite positive values")
    if not volume.is_finite() or volume < 0 or tick_count < 0:
        raise ValueError("Volume and trade count must be non-negative")
    if high_price < max(open_price, close_price) or low_price > min(
        open_price, close_price
    ):
        raise ValueError("Kline high/low values are inconsistent with open/close")
    if high_price < low_price:
        raise ValueError("Kline high price is lower than low price")

    trade_date = datetime.fromtimestamp(int(raw[0]) / 1000, tz=UTC).date()
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "market_type": "CRYPTO",
        "source": SOURCE,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "tick_count": tick_count,
    }


def _fetch_page(symbol: str, from_date: date, to_date: date) -> list[list[Any]]:
    query = urlencode(
        {
            "symbol": symbol,
            "interval": "1d",
            "startTime": _utc_millis(from_date),
            "endTime": _utc_millis(to_date, end_of_day=True),
            "timeZone": "0",
            "limit": 1000,
        }
    )
    request = Request(
        f"{BINANCE_KLINES_URL}?{query}",
        headers={"User-Agent": "StockFlow daily backfill/1.0"},
    )
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)

    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected Binance response: {payload}")
    return payload


def fetch_klines(symbol: str, from_date: date, to_date: date) -> list[dict[str, Any]]:
    validate_range(from_date, to_date)
    rows: list[dict[str, Any]] = []
    cursor = from_date

    while cursor <= to_date:
        page_end = min(to_date, cursor + timedelta(days=999))
        payload = _fetch_page(symbol, cursor, page_end)
        parsed = [parse_kline(symbol, raw) for raw in payload]
        rows.extend(
            row for row in parsed if cursor <= row["trade_date"] <= page_end
        )
        cursor = page_end + timedelta(days=1)

    rows.sort(key=lambda row: row["trade_date"])
    return rows


def select_missing_rows(
    fetched_rows: Iterable[dict[str, Any]], existing_dates: set[date]
) -> list[dict[str, Any]]:
    return [
        row for row in fetched_rows if row["trade_date"] not in existing_dates
    ]


def load_existing_dates(symbol: str, from_date: date, to_date: date) -> set[date]:
    engine = get_engine()
    with engine.connect() as connection:
        result = connection.execute(
            EXISTING_DATES_SQL,
            {
                "symbol": symbol,
                "source": SOURCE,
                "from_date": from_date,
                "to_date": to_date,
            },
        )
        return set(result.scalars())


def insert_missing_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    engine = get_engine()
    with engine.begin() as connection:
        result = connection.execute(INSERT_SQL, rows)
    return max(result.rowcount or 0, 0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or insert missing Binance daily candles."
    )
    parser.add_argument("--symbol", required=True, help="Binance symbol, e.g. BTCUSDT")
    parser.add_argument("--from", dest="from_date", required=True, type=parse_iso_date)
    parser.add_argument("--to", dest="to_date", required=True, type=parse_iso_date)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Insert missing rows. Without this flag the command is read-only.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    symbol = args.symbol.strip().upper()
    if not symbol:
        raise SystemExit("symbol must not be empty")

    try:
        validate_range(args.from_date, args.to_date)
        fetched_rows = fetch_klines(symbol, args.from_date, args.to_date)
        existing_dates = load_existing_dates(symbol, args.from_date, args.to_date)
        missing_rows = select_missing_rows(fetched_rows, existing_dates)
        inserted = insert_missing_rows(missing_rows) if args.apply else 0
    except (RuntimeError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc

    summary = {
        "mode": "apply" if args.apply else "dry-run",
        "symbol": symbol,
        "from": args.from_date,
        "to": args.to_date,
        "fetched_count": len(fetched_rows),
        "existing_count": len(existing_dates),
        "missing_count": len(missing_rows),
        "missing_dates": [row["trade_date"] for row in missing_rows],
        "inserted_count": inserted,
    }
    print(json.dumps(summary, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
