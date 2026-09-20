"""
환경 설정 관리 모듈
환경 변수 또는 .env 파일에서 설정을 로드
"""
import logging
import os
from typing import Optional
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

logger = logging.getLogger(__name__)


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
    # librdkafka는 "all"을 숫자 -1로도 받는다. enable.idempotence=True(아래 참고)와
    # 호환되려면 -1(all)이어야 하므로 기본값도 -1로 둔다.
    # 연산자가 "all" 문자열을 쓰고 싶다면 -1을 대신 사용할 것.
    KAFKA_ACKS: int = int(os.getenv('KAFKA_ACKS', '-1'))
    KAFKA_RETRIES: int = int(os.getenv('KAFKA_RETRIES', '5'))
    KAFKA_REQUEST_TIMEOUT_MS: int = int(os.getenv('KAFKA_REQUEST_TIMEOUT_MS', '30000'))
    
    # Binance 설정
    BINANCE_TOP_SYMBOLS_LIMIT: int = int(os.getenv('BINANCE_TOP_SYMBOLS_LIMIT', '300'))
    BINANCE_SYMBOL_REFRESH_INTERVAL_HOURS: int = int(os.getenv('BINANCE_SYMBOL_REFRESH_INTERVAL_HOURS', '1'))
    BINANCE_TOPIC_NAME: str = os.getenv('BINANCE_TOPIC_NAME', 'market.binance.tick')
    
    # Alpaca 설정
    ALPACA_API_KEY: Optional[str] = os.getenv('ALPACA_API_KEY')
    ALPACA_API_SECRET: Optional[str] = os.getenv('ALPACA_API_SECRET')
    ALPACA_WEBSOCKET_URL: str = os.getenv(
        'ALPACA_WEBSOCKET_URL', 
        'wss://stream.data.alpaca.markets/v2/iex'
    )
    ALPACA_TOPIC_NAME: str = os.getenv('ALPACA_TOPIC_NAME', 'market.alpaca.tick')
    
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

    # 헬스체크 설정 (Docker HEALTHCHECK가 읽는 파일)
    HEALTH_FILE_PATH: str = os.getenv('HEALTH_FILE_PATH', '/tmp/collector_health.json')
    HEALTH_STALE_THRESHOLD_SEC: int = int(os.getenv('HEALTH_STALE_THRESHOLD_SEC', '120'))
    HEALTH_WRITE_MIN_INTERVAL_SEC: float = float(os.getenv('HEALTH_WRITE_MIN_INTERVAL_SEC', '1.0'))

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

        # Binance/Alpaca가 같은 토픽으로 설정되면 두 소스의 데이터가 섞여
        # market.binance.tick / market.alpaca.tick에 설계된 파티션 수·retention이
        # 무의미해지고, 파티션 키(symbol) 충돌 시 순서 보장도 깨질 수 있다.
        if cls.BINANCE_TOPIC_NAME == cls.ALPACA_TOPIC_NAME:
            errors.append(
                f"BINANCE_TOPIC_NAME과 ALPACA_TOPIC_NAME이 동일합니다 "
                f"('{cls.BINANCE_TOPIC_NAME}'). 두 거래소는 서로 다른 토픽을 사용해야 "
                f"합니다 (예: market.binance.tick / market.alpaca.tick)"
            )

        if errors:
            for error in errors:
                print(f"❌ 설정 오류: {error}")
            return False
        
        return True
    
    @classmethod
    def get_kafka_producer_config(cls, client_id_suffix: str) -> dict:
        """Kafka Producer 설정 딕셔너리 반환"""
        # enable.idempotence=True는 KAFKA_TOPIC_DESIGN.md가 약속하는 종목별(symbol
        # 파티션 키) 순서 보장을 위해 필요하다: retries가 있는 상태에서 idempotence
        # 없이는 재시도된 배치가 이후 배치보다 늦게 도착해 순서가 뒤바뀔 수 있다.
        # librdkafka는 idempotence 활성화 시 acks=all(-1)을 요구하므로, 다른 값이
        # 설정되어 있으면 강제로 -1로 보정한다(그렇지 않으면 Producer 생성 자체가
        # 실패한다). max.in.flight.requests.per.connection은 명시적으로 지정하지
        # 않으면 librdkafka가 idempotence 활성화 시 자동으로 5 이하로 낮춘다.
        acks = cls.KAFKA_ACKS
        if acks not in (-1, 'all'):
            logger.warning(
                f"⚠️ KAFKA_ACKS={acks}는 enable.idempotence=True와 호환되지 않아 "
                f"-1(all)로 강제 적용합니다 (종목별 순서 보장을 위해 idempotence가 우선)"
            )
            acks = -1

        return {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f"{cls.KAFKA_CLIENT_ID_PREFIX}-{client_id_suffix}",
            'linger.ms': cls.KAFKA_LINGER_MS,
            'batch.num.messages': cls.KAFKA_BATCH_NUM_MESSAGES,
            'queue.buffering.max.messages': cls.KAFKA_QUEUE_BUFFERING_MAX_MESSAGES,
            'compression.type': cls.KAFKA_COMPRESSION_TYPE,
            'acks': acks,
            'retries': cls.KAFKA_RETRIES,
            'request.timeout.ms': cls.KAFKA_REQUEST_TIMEOUT_MS,
            'enable.idempotence': True,
        }
