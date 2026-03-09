"""
데이터 정규화 모듈
다양한 거래소의 데이터를 NormalizedTradeDTO 형식으로 변환
"""
import json
import logging
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


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
            
            received_at = int(datetime.utcnow().timestamp() * 1000)
            
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
            
            # 타임스탬프 변환 (ISO 8601 -> epoch milliseconds)
            try:
                # ISO 8601 형식: "2023-01-01T12:00:00Z" 또는 "2023-01-01T12:00:00.123Z"
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp() * 1000)
            except (ValueError, AttributeError):
                # 파싱 실패 시 현재 시간 사용
                timestamp = int(datetime.utcnow().timestamp() * 1000)
                logger.warning(f"Alpaca 타임스탬프 파싱 실패, 현재 시간 사용: {timestamp_str}")
            
            # Trade ID 생성 (exchange + timestamp + symbol 조합)
            # Alpaca는 고유 trade ID를 제공하지 않으므로 조합하여 생성
            trade_id = f"{exchange}_{timestamp}_{symbol}_{json_data.get('s', 0)}"
            
            received_at = int(datetime.utcnow().timestamp() * 1000)
            
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
            
            # 타임스탬프 변환
            try:
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                timestamp = int(dt.timestamp() * 1000)
            except (ValueError, AttributeError):
                timestamp = int(datetime.utcnow().timestamp() * 1000)
            
            # Trade ID 생성
            trade_id = f"{exchange}_QUOTE_{timestamp}_{symbol}"
            
            received_at = int(datetime.utcnow().timestamp() * 1000)
            
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
