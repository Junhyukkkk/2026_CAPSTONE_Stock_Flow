"""
데이터 정규화 모듈
다양한 거래소의 데이터를 NormalizedTradeDTO 형식으로 변환
"""
import json
import logging
import re
import time
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# 소수점 이하 초(fractional seconds) 부분을 찾기 위한 정규식
_FRACTIONAL_SECONDS_RE = re.compile(r'\.(\d+)')


def _normalize_fractional_seconds(timestamp_str: str) -> str:
    """
    ISO 8601 타임스탬프의 소수점 이하 자릿수를 6자리(마이크로초)로 정규화.

    Python 3.9/3.10의 datetime.fromisoformat()은 소수점 이하가 0, 3, 6자리인
    경우만 파싱 가능하다. Alpaca의 "t" 필드는 나노초 정밀도(9자리)의 RFC-3339
    타임스탬프이므로, 그대로 넘기면 3.9/3.10에서 ValueError가 발생한다.
    이를 방지하기 위해 자릿수와 무관하게 항상 6자리로 잘라내거나 0으로 채운다.
    (Python 3.11+ 는 임의 자릿수를 허용하지만, 이 로직은 모든 버전에서 안전하게 동작한다.)
    """
    match = _FRACTIONAL_SECONDS_RE.search(timestamp_str)
    if not match:
        return timestamp_str

    fractional_digits = match.group(1)
    normalized = (fractional_digits + '000000')[:6]  # 부족하면 0으로 채우고, 넘치면 잘라냄
    start, end = match.span(1)
    return timestamp_str[:start] + normalized + timestamp_str[end:]


def _parse_alpaca_timestamp_ms(timestamp_str: str) -> int:
    """
    Alpaca RFC-3339 타임스탬프("2023-01-01T12:00:00.123456789Z" 등)를
    epoch milliseconds로 변환.

    파싱이 정말로 불가능한 경우(형식 자체가 잘못된 경우 등)에는 폴백으로
    현재 처리 시각(processing time)을 사용하되, 이는 실제 체결/호가 시각이
    아니므로 반드시 경고 로그로 명확히 남긴다.
    """
    try:
        iso_str = _normalize_fractional_seconds(timestamp_str.replace('Z', '+00:00'))
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)
    except (ValueError, AttributeError, TypeError) as e:
        # time.time()을 사용: datetime.utcnow().timestamp()는 naive datetime을
        # "로컬 시간"으로 오인해 timestamp()를 계산하므로, 시스템 로컬 타임존이
        # UTC가 아니면(예: KST) 그 오프셋만큼 값이 어긋나는 잘 알려진 함정이 있다.
        fallback_ts = int(time.time() * 1000)
        logger.warning(
            f"⚠️ Alpaca 타임스탬프 파싱 실패 - 실제 체결/호가 시각이 아닌 "
            f"'처리 시각(현재 시각)'으로 대체합니다 (순서/지연시간/캔들 집계에 영향 가능). "
            f"원본 값='{timestamp_str}', 오류={e}"
        )
        return fallback_ts


class NormalizedTradeDTO:
    """
    정규화된 거래 데이터 DTO
    Java의 NormalizedTradeDTO와 동일한 구조
    """
    
    def __init__(
        self,
        source: str,
        symbol: str,
        price: Decimal,
        volume: Decimal,
        trade_id: str,
        exchange: str,
        timestamp: int,
        received_at: int,
        market_type: str
    ):
        self.source = source
        self.symbol = symbol
        self.price = price
        self.volume = volume
        self.trade_id = trade_id
        self.exchange = exchange
        self.timestamp = timestamp
        self.received_at = received_at
        self.market_type = market_type
    
    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (Kafka 전송용)"""
        return {
            "source": self.source,
            "symbol": self.symbol,
            "price": str(self.price),  # BigDecimal 호환을 위해 문자열로
            "volume": str(self.volume),
            "tradeId": self.trade_id,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "receivedAt": self.received_at,
            "marketType": self.market_type
        }
    
    def to_json(self) -> str:
        """JSON 문자열로 변환"""
        return json.dumps(self.to_dict())


class DataNormalizer:
    """데이터 정규화 클래스"""
    
    @staticmethod
    def normalize_binance_data(json_data: Dict[str, Any]) -> Optional[NormalizedTradeDTO]:
        """
        Binance WebSocket 데이터를 NormalizedTradeDTO로 변환
        
        Binance aggTrade 스트림 형식:
        {
            "e": "aggTrade",
            "E": 123456789,
            "s": "BTCUSDT",
            "a": 12345,
            "p": "0.001",
            "q": "100",
            "f": 100,
            "l": 105,
            "T": 123456785,
            "m": true
        }
        """
        try:
            # Binance는 stream 형식과 단일 메시지 형식 모두 지원
            raw = json_data.get('data', json_data)
            
            # 필수 필드 검증
            if not all(key in raw for key in ['s', 'p', 'q', 'T', 'a']):
                return None
            
            symbol = raw['s']
            price = Decimal(str(raw['p']))
            quantity = Decimal(str(raw['q']))
            timestamp = raw['T']  # 이미 milliseconds
            trade_id = str(raw['a'])  # aggregate trade ID
            
            # 유효성 검증
            if price <= 0 or quantity <= 0:
                return None
            
            # datetime.utcnow().timestamp()는 로컬 타임존이 UTC가 아니면 값이
            # 어긋나는 함정이 있어(예: KST 환경에서 9시간 오차) time.time()을 사용
            received_at = int(time.time() * 1000)
            
            return NormalizedTradeDTO(
                source="BINANCE",
                symbol=symbol,
                price=price,
                volume=quantity,
                trade_id=trade_id,
                exchange="BINANCE",
                timestamp=timestamp,
                received_at=received_at,
                market_type="CRYPTO"
            )
            
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Binance 데이터 정규화 실패: {e}, 데이터: {json_data}")
            return None
    
    @staticmethod
    def normalize_alpaca_trade_data(json_data: Dict[str, Any]) -> Optional[NormalizedTradeDTO]:
        """
        Alpaca Trade 메시지를 NormalizedTradeDTO로 변환
        
        Alpaca Trade 형식:
        {
            "T": "t",
            "S": "AAPL",
            "p": 150.25,
            "s": 100,
            "t": "2023-01-01T12:00:00Z",
            "x": "IEX",
            "c": ["@I", "E"]
        }
        """
        try:
            # 필수 필드 검증
            if not all(key in json_data for key in ['S', 'p', 's', 't']):
                return None
            
            symbol = json_data['S']
            price = Decimal(str(json_data['p']))
            size = Decimal(str(json_data['s']))
            timestamp_str = json_data['t']
            exchange = json_data.get('x', 'UNKNOWN')
            
            # 유효성 검증
            if price <= 0 or size <= 0:
                return None
            
            # 타임스탬프 변환 (ISO 8601, 나노초 정밀도 포함 -> epoch milliseconds)
            timestamp = _parse_alpaca_timestamp_ms(timestamp_str)

            # Trade ID: Alpaca 메시지의 "i" 필드가 실제 고유 거래 ID이므로 우선 사용.
            # "i" 필드가 없는 경우에만 exchange+timestamp+symbol+size 조합으로 합성
            # ID를 생성한다(드묾, size 기반이라 서로 다른 체결끼리 충돌할 수 있음).
            raw_trade_id = json_data.get('i')
            if raw_trade_id is not None:
                trade_id = str(raw_trade_id)
            else:
                trade_id = f"{exchange}_{timestamp}_{symbol}_{json_data.get('s', 0)}"
            
            # datetime.utcnow().timestamp()는 로컬 타임존이 UTC가 아니면 값이
            # 어긋나는 함정이 있어(예: KST 환경에서 9시간 오차) time.time()을 사용
            received_at = int(time.time() * 1000)
            
            return NormalizedTradeDTO(
                source="ALPACA",
                symbol=symbol,
                price=price,
                volume=size,
                trade_id=trade_id,
                exchange=exchange,
                timestamp=timestamp,
                received_at=received_at,
                market_type="STOCK"
            )
            
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Alpaca Trade 데이터 정규화 실패: {e}, 데이터: {json_data}")
            return None
    
    @staticmethod
    def normalize_alpaca_quote_data(json_data: Dict[str, Any]) -> Optional[NormalizedTradeDTO]:
        """
        Alpaca Quote 메시지를 NormalizedTradeDTO로 변환
        Quote는 체결이 아니므로, mid price를 사용하여 가상의 체결로 변환
        
        주의: Quote는 실제 체결이 아니므로, 가능하면 Trade만 사용하는 것을 권장
        """
        try:
            # 필수 필드 검증
            if not all(key in json_data for key in ['S', 'bp', 'ap', 't']):
                return None
            
            symbol = json_data['S']
            bid_price = Decimal(str(json_data.get('bp', 0)))
            ask_price = Decimal(str(json_data.get('ap', 0)))
            
            # 유효성 검증
            if bid_price <= 0 or ask_price <= 0:
                return None
            
            # Mid price 계산 (bid와 ask의 중간값)
            price = (bid_price + ask_price) / 2
            volume = Decimal('0')  # Quote는 체결이 아니므로 volume은 0
            
            timestamp_str = json_data['t']
            exchange = json_data.get('bx', json_data.get('ax', 'UNKNOWN'))
            
            # 타임스탬프 변환 (ISO 8601, 나노초 정밀도 포함 -> epoch milliseconds)
            timestamp = _parse_alpaca_timestamp_ms(timestamp_str)

            # Trade ID 생성
            trade_id = f"{exchange}_QUOTE_{timestamp}_{symbol}"
            
            # datetime.utcnow().timestamp()는 로컬 타임존이 UTC가 아니면 값이
            # 어긋나는 함정이 있어(예: KST 환경에서 9시간 오차) time.time()을 사용
            received_at = int(time.time() * 1000)
            
            return NormalizedTradeDTO(
                source="ALPACA",
                symbol=symbol,
                price=price,
                volume=volume,
                trade_id=trade_id,
                exchange=exchange,
                timestamp=timestamp,
                received_at=received_at,
                market_type="STOCK"
            )
            
        except (KeyError, ValueError, TypeError) as e:
            logger.debug(f"Alpaca Quote 데이터 정규화 실패: {e}, 데이터: {json_data}")
            return None
