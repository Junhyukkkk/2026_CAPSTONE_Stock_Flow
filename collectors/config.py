"""
환경 설정 관리 모듈
환경 변수 또는 .env 파일에서 설정을 로드
"""
import os
from typing import Optional
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()


class Config:
    """애플리케이션 설정"""
    
    # Kafka 설정
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_CLIENT_ID_PREFIX: str = os.getenv('KAFKA_CLIENT_ID_PREFIX', 'stockflow-collector')
    
    # Kafka Producer 설정
    KAFKA_LINGER_MS: int = int(os.getenv('KAFKA_LINGER_MS', '10'))
    KAFKA_BATCH_NUM_MESSAGES: int = int(os.getenv('KAFKA_BATCH_NUM_MESSAGES', '1000'))
    KAFKA_QUEUE_BUFFERING_MAX_MESSAGES: int = int(os.getenv('KAFKA_QUEUE_BUFFERING_MAX_MESSAGES', '200000'))
    KAFKA_COMPRESSION_TYPE: str = os.getenv('KAFKA_COMPRESSION_TYPE', 'snappy')
    KAFKA_ACKS: int = int(os.getenv('KAFKA_ACKS', '1'))
    KAFKA_RETRIES: int = int(os.getenv('KAFKA_RETRIES', '5'))
    KAFKA_REQUEST_TIMEOUT_MS: int = int(os.getenv('KAFKA_REQUEST_TIMEOUT_MS', '30000'))
    
    # Binance 설정
    BINANCE_TOP_SYMBOLS_LIMIT: int = int(os.getenv('BINANCE_TOP_SYMBOLS_LIMIT', '300'))
    BINANCE_SYMBOL_REFRESH_INTERVAL_HOURS: int = int(os.getenv('BINANCE_SYMBOL_REFRESH_INTERVAL_HOURS', '1'))
    BINANCE_TOPIC_NAME: str = os.getenv('BINANCE_TOPIC_NAME', 'market.normalized')
    
    # Alpaca 설정
    ALPACA_API_KEY: Optional[str] = os.getenv('ALPACA_API_KEY')
    ALPACA_API_SECRET: Optional[str] = os.getenv('ALPACA_API_SECRET')
    ALPACA_WEBSOCKET_URL: str = os.getenv(
        'ALPACA_WEBSOCKET_URL', 
        'wss://stream.data.alpaca.markets/v2/iex'
    )
    ALPACA_TOPIC_NAME: str = os.getenv('ALPACA_TOPIC_NAME', 'market.normalized')
    
    # DLQ 설정
    DLQ_TOPIC_NAME: str = os.getenv('DLQ_TOPIC_NAME', 'market.dlq')
    DLQ_ENABLED: bool = os.getenv('DLQ_ENABLED', 'true').lower() == 'true'
    ALPACA_SUBSCRIBE_ALL_STOCKS: bool = os.getenv('ALPACA_SUBSCRIBE_ALL_STOCKS', 'true').lower() == 'true'
    ALPACA_SUBSCRIBE_SYMBOLS: str = os.getenv('ALPACA_SUBSCRIBE_SYMBOLS', '')  # 쉼표로 구분된 종목 리스트
    
    # 재연결 설정
    RECONNECT_INITIAL_DELAY_SEC: int = int(os.getenv('RECONNECT_INITIAL_DELAY_SEC', '5'))
    RECONNECT_MAX_DELAY_SEC: int = int(os.getenv('RECONNECT_MAX_DELAY_SEC', '60'))
    RECONNECT_BACKOFF_MULTIPLIER: float = float(os.getenv('RECONNECT_BACKOFF_MULTIPLIER', '2.0'))
    
    # 로깅 설정
    LOG_LEVEL: str = os.getenv('LOG_LEVEL', 'INFO')
    LOG_STATS_INTERVAL: int = int(os.getenv('LOG_STATS_INTERVAL', '5000'))  # N개 메시지마다 통계 출력
    
    # 메트릭 설정
    METRICS_ENABLED: bool = os.getenv('METRICS_ENABLED', 'true').lower() == 'true'
    
    @classmethod
    def validate(cls) -> bool:
        """필수 설정 검증"""
        errors = []
        
        if not cls.KAFKA_BOOTSTRAP_SERVERS:
            errors.append("KAFKA_BOOTSTRAP_SERVERS가 설정되지 않았습니다")
        
        # Alpaca는 선택사항이지만, 설정되어 있으면 키와 시크릿이 모두 필요
        if cls.ALPACA_API_KEY and not cls.ALPACA_API_SECRET:
            errors.append("ALPACA_API_SECRET이 설정되지 않았습니다")
        if cls.ALPACA_API_SECRET and not cls.ALPACA_API_KEY:
            errors.append("ALPACA_API_KEY가 설정되지 않았습니다")
        
        if errors:
            for error in errors:
                print(f"❌ 설정 오류: {error}")
            return False
        
        return True
    
    @classmethod
    def get_kafka_producer_config(cls, client_id_suffix: str) -> dict:
        """Kafka Producer 설정 딕셔너리 반환"""
        return {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f"{cls.KAFKA_CLIENT_ID_PREFIX}-{client_id_suffix}",
            'linger.ms': cls.KAFKA_LINGER_MS,
            'batch.num.messages': cls.KAFKA_BATCH_NUM_MESSAGES,
            'queue.buffering.max.messages': cls.KAFKA_QUEUE_BUFFERING_MAX_MESSAGES,
            'compression.type': cls.KAFKA_COMPRESSION_TYPE,
            'acks': cls.KAFKA_ACKS,
            'retries': cls.KAFKA_RETRIES,
            'request.timeout.ms': cls.KAFKA_REQUEST_TIMEOUT_MS,
        }
