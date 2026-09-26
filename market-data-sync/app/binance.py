"""Binance 공개 캔들 데이터 클라이언트 (API 키 불필요).

- 대량 과거 1분봉: data.binance.vision 월별/일별 zip (SHA256 체크섬 검증)
- 일봉 · 최근 구간: REST /api/v3/klines

아카이브 CSV 는 헤더가 없고, 2025-01-01 이후 파일의 시각은 마이크로초 단위다.
"""
import csv
import hashlib
import io
import json
import logging
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import quote

from .config import settings

log = logging.getLogger(__name__)

ARCHIVE = "https://data.binance.vision/data/spot"
REST = "https://api.binance.com/api/v3"


@dataclass(frozen=True)
class Candle:
    open_time: datetime
    open: str
    high: str
    low: str
    close: str
    volume: str
    close_time: datetime
    quote_volume: str
    trade_count: int


def to_datetime(raw) -> datetime:
    """밀리초(13자리)와 마이크로초(16자리) 타임스탬프를 모두 UTC datetime 으로 바꾼다."""
    value = int(raw)
    seconds = value / 1_000_000 if value >= 10**14 else value / 1_000
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def parse_row(row) -> Candle:
    return Candle(
        open_time=to_datetime(row[0]), open=str(row[1]), high=str(row[2]), low=str(row[3]),
        close=str(row[4]), volume=str(row[5]), close_time=to_datetime(row[6]),
        quote_volume=str(row[7]), trade_count=int(row[8]),
    )


def parse_csv(text: str) -> list:
    """아카이브 CSV → Candle 목록. 숫자로 시작하지 않는 줄(혹시 모를 헤더)은 건너뛴다."""
    return [parse_row(r) for r in csv.reader(io.StringIO(text)) if r and r[0].isdigit()]


def _get(url: str, timeout: int = 60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "stockflow-market-data-sync"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        weight = response.headers.get("x-mbx-used-weight-1m")
        if weight and int(weight) > settings.binance_weight_soft_limit:
            log.warning("binance weight %s > %s, backing off 30s", weight, settings.binance_weight_soft_limit)
            time.sleep(30)
        return response.read()


def archive_url(symbol: str, interval: str, period: str) -> str:
    """period 가 'YYYY-MM' 이면 월별, 'YYYY-MM-DD' 이면 일별 파일."""
    kind = "monthly" if len(period) == 7 else "daily"
    s = quote(symbol)  # 중국어 이름 종목(예: 币安人生USDT)도 있다
    return f"{ARCHIVE}/{kind}/klines/{s}/{interval}/{s}-{interval}-{period}.zip"


def download_archive(symbol: str, interval: str, period: str):
    """아카이브 파일을 받아 Candle 목록을 반환한다. 파일이 없으면(상장 전 등) None."""
    url = archive_url(symbol, interval, period)
    try:
        payload = _get(url)
        expected = _get(url + ".CHECKSUM", timeout=30).decode().split()[0]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    actual = hashlib.sha256(payload).hexdigest()
    if actual != expected:
        raise ValueError(f"checksum mismatch for {url}")
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        text = archive.read(archive.namelist()[0]).decode()
    time.sleep(settings.archive_pause_seconds)
    return parse_csv(text)


def rest_klines(symbol: str, interval: str, start: datetime, end: datetime) -> list:
    """[start, end) 구간의 확정된 캔들만 반환한다(진행 중인 캔들 제외)."""
    now = datetime.now(timezone.utc)
    out, cursor = [], int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)
    while cursor < end_ms:
        url = (f"{REST}/klines?symbol={quote(symbol)}&interval={interval}"
               f"&startTime={cursor}&endTime={end_ms - 1}&limit=1000")
        rows = json.loads(_get(url, timeout=30))
        if not rows:
            break
        candles = [parse_row(r) for r in rows]
        out.extend(c for c in candles if c.close_time < now)
        cursor = int(rows[-1][0]) + 1
        time.sleep(settings.rest_pause_seconds)
    return out


def usdt_symbols() -> list:
    """현재 거래 중인 USDT 현물 마켓 심볼."""
    info = json.loads(_get(f"{REST}/exchangeInfo?permissions=SPOT", timeout=60))
    return sorted(s["symbol"] for s in info["symbols"]
                  if s["quoteAsset"] == "USDT" and s["status"] == "TRADING")


def month_periods(first: date, last: date) -> list:
    """first 가 속한 달부터 last 가 속한 달까지 'YYYY-MM' 목록."""
    periods, y, m = [], first.year, first.month
    while (y, m) <= (last.year, last.month):
        periods.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return periods
