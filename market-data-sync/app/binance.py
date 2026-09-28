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
import re
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


LISTING = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

# 코인이 아닌 USDT 거래쌍: 레버리지 토큰, 스테이블코인, 법정화폐, 래핑 · 금 토큰
# Binance 레버리지 토큰은 정해진 기초 자산에만 있었다. 접미사만 보면 JUPUSDT 같은 실제 코인까지 걸린다.
_LEVERAGED = re.compile(
    r"^(BTC|ETH|BNB|ADA|XRP|LINK|DOT|TRX|EOS|XTZ|LTC|SXP|FIL|YFI|SUSHI|UNI|XLM|BCH|AAVE|1INCH)"
    r"(UP|DOWN)USDT$|^(ETH|BNB|EOS|XRP)?(BULL|BEAR)USDT$")
NON_COIN = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT", "PAXUSDT",
            "USDSUSDT", "USDSBUSDT", "SUSDUSDT", "USTUSDT", "USDEUSDT", "USD1USDT", "XUSDUSDT",
            "RLUSDUSDT", "BFUSDUSDT", "AEURUSDT", "EURIUSDT", "EURUSDT", "GBPUSDT", "AUDUSDT",
            "BRLUSDT", "TRYUSDT", "RUBUSDT", "UAHUSDT", "BIDRUSDT", "IDRTUSDT", "NGNUSDT",
            "ZARUSDT", "ARSUSDT", "BKRWUSDT", "PAXGUSDT", "XAUTUSDT", "WBTCUSDT", "WBETHUSDT",
            "BETHUSDT", "BNSOLUSDT"}


def is_coin_pair(symbol: str) -> bool:
    return (symbol.endswith("USDT") and symbol != "USDT" and symbol not in NON_COIN
            and not _LEVERAGED.match(symbol))


def _list_keys(prefix: str, delimiter: str = None) -> list:
    """data.binance.vision(S3) 목록. delimiter 를 주면 하위 '폴더' 이름, 아니면 파일 키."""
    items, marker = [], ""
    while True:
        url = f"{LISTING}?prefix={quote(prefix)}" + (f"&delimiter={delimiter}" if delimiter else "") \
              + (f"&marker={quote(marker)}" if marker else "")
        xml = _get(url, timeout=60).decode()
        tag = "Prefix" if delimiter else "Key"
        found = re.findall(rf"<{tag}>([^<]+)</{tag}>", xml)
        found = [f for f in found if f != prefix]
        items += found
        if "<IsTruncated>true</IsTruncated>" not in xml or not found:
            return items
        marker = found[-1]


def archive_usdt_pairs() -> list:
    """아카이브에 있는 USDT 현물 거래쌍(상장폐지 종목 포함), 코인이 아닌 쌍은 제외."""
    base = "data/spot/monthly/klines/"
    names = [p[len(base):].strip("/") for p in _list_keys(base, delimiter="/")]
    return sorted(s for s in names if is_coin_pair(s))


def archive_daily_periods(symbol: str) -> list:
    """일봉 아카이브 기간: 완료된 달은 월별 파일, 그 뒤(상장폐지 직전 달 등)는 일별 파일.

    상장폐지 직전의 폭락 구간이 생존 편향에서 가장 중요하므로 마지막 부분 달을 놓치지 않는다.
    """
    s = symbol
    monthly_prefix = f"data/spot/monthly/klines/{s}/1d/"
    months = sorted(re.findall(rf"{re.escape(s)}-1d-(\d{{4}}-\d{{2}})\.zip$", k)[0]
                    for k in _list_keys(monthly_prefix) if k.endswith(".zip"))
    periods = list(months)
    if months:
        y, m = map(int, months[-1].split("-"))
        tail = []
        for _ in range(2):  # 마지막 월별 파일 다음 두 달의 일별 파일
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
            prefix = f"data/spot/daily/klines/{s}/1d/{s}-1d-{y:04d}-{m:02d}"
            tail += sorted(re.findall(rf"{re.escape(s)}-1d-(\d{{4}}-\d{{2}}-\d{{2}})\.zip$", k)[0]
                           for k in _list_keys(prefix) if k.endswith(".zip"))
        periods += tail
    return periods


def month_periods(first: date, last: date) -> list:
    """first 가 속한 달부터 last 가 속한 달까지 'YYYY-MM' 목록."""
    periods, y, m = [], first.year, first.month
    while (y, m) <= (last.year, last.month):
        periods.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return periods
