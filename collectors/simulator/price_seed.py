"""
시작가 로더 — SIM_PRICE_SOURCE 에 따라 CSV 시작가를 Alpaca 실제 시세로 덮어쓴다.

어떤 실패(네트워크·401·빈 응답·예상 밖 JSON)도 예외로 전파하지 않고 CSV 값으로 폴백한다.
실제 시세는 "시작가"로만 쓰이며 이후 체결은 모두 시뮬레이션이다 (source=SIMULATOR).
"""
import dataclasses
import logging
import math
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import requests

from simulator.universe import Instrument

logger = logging.getLogger(__name__)

SNAPSHOTS_URL = 'https://data.alpaca.markets/v2/stocks/snapshots'
CHUNK_SIZE = 50
REQUEST_TIMEOUT_SEC = 10.0
# CSV 가격 대비 이 범위를 벗어난 실시세는 분할·오염 데이터로 보고 무시한다
MIN_PRICE_RATIO = 0.2
MAX_PRICE_RATIO = 5.0


def _positive_price(value) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    price = float(value)
    return price if math.isfinite(price) and price > 0 else None


def _snapshot_price(snapshot, reference: float) -> Optional[float]:
    """latestTrade.p → dailyBar.c → prevDailyBar.c 순으로 읽어, 처음 나오는 정상 범위의 가격을 반환한다."""
    if not isinstance(snapshot, dict):
        return None
    for key, field in (('latestTrade', 'p'), ('dailyBar', 'c'), ('prevDailyBar', 'c')):
        section = snapshot.get(key)
        price = _positive_price(section.get(field)) if isinstance(section, dict) else None
        if price is not None and MIN_PRICE_RATIO * reference <= price <= MAX_PRICE_RATIO * reference:
            return price
    return None


def _chunks(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _fetch_snapshots(
    symbols: Sequence[str], api_key: str, api_secret: str, http_get: Callable, timeout: float
) -> Dict[str, dict]:
    """심볼 → 스냅샷. 실패한 청크는 건너뛰고(로그), 인증 실패면 즉시 중단한다."""
    headers = {'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': api_secret}
    result: Dict[str, dict] = {}
    for chunk in _chunks(symbols, CHUNK_SIZE):
        try:
            response = http_get(
                SNAPSHOTS_URL,
                params={'symbols': ','.join(chunk), 'feed': 'iex'},
                headers=headers,
                timeout=timeout,
            )
            if response.status_code in (401, 403):
                logger.warning(f"⚠️ Alpaca 인증 실패(HTTP {response.status_code}) — 실제 시세 읽기를 중단합니다")
                break
            if response.status_code != 200:
                logger.warning(f"⚠️ Alpaca 스냅샷 응답 오류(HTTP {response.status_code}) — 해당 {len(chunk)}종목은 CSV 가격 사용")
                continue
            body = response.json()
        except Exception as e:  # 네트워크·타임아웃·JSON 파싱 등 어떤 오류도 시뮬레이터를 죽이지 않는다
            logger.warning(f"⚠️ Alpaca 스냅샷 요청 실패({type(e).__name__}: {e}) — 해당 {len(chunk)}종목은 CSV 가격 사용")
            continue

        if isinstance(body, dict) and isinstance(body.get('snapshots'), dict):
            body = body['snapshots']  # 응답 래핑 형태 차이를 흡수
        if not isinstance(body, dict):
            logger.warning("⚠️ Alpaca 스냅샷 응답 형식이 예상과 달라 무시합니다")
            continue
        for symbol, snapshot in body.items():
            if isinstance(symbol, str):
                result[symbol.upper()] = snapshot
    return result


def seed_prices(
    instruments: List[Instrument],
    source: str,
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    http_get: Callable = requests.get,
    timeout: float = REQUEST_TIMEOUT_SEC,
) -> List[Instrument]:
    """source: static(CSV 그대로) | alpaca(실시세, 실패 시 CSV) | auto(키가 없으면 시도하지 않고 CSV).
    읽지 못한 종목은 CSV 시작가를 유지한다."""
    if source == 'static':
        return instruments

    if not (api_key and api_secret):
        if source == 'alpaca':
            logger.warning("⚠️ SIM_PRICE_SOURCE=alpaca 이지만 ALPACA_API_KEY/ALPACA_API_SECRET 이 없어 CSV 시작가를 사용합니다")
        else:
            logger.info("ℹ️ Alpaca 키가 없어 CSV 시작가를 사용합니다 (SIM_PRICE_SOURCE=auto)")
        return instruments

    try:
        snapshots = _fetch_snapshots([i.symbol for i in instruments], api_key, api_secret, http_get, timeout)
        seeded = []
        live = 0
        for instrument in instruments:
            price = _snapshot_price(snapshots.get(instrument.symbol), instrument.price)
            if price is None:
                seeded.append(instrument)
            else:
                seeded.append(dataclasses.replace(instrument, price=price))
                live += 1
    except Exception:  # 방어: 로더 버그가 시뮬레이터 기동을 막지 않게 한다
        logger.exception("⚠️ 시작가 로더 오류 — 전 종목 CSV 시작가를 사용합니다")
        return instruments

    logger.info(f"📈 시작가: Alpaca 실시세 {live}개 / CSV {len(instruments) - live}개 (이후 체결은 모두 시뮬레이션)")
    return seeded
