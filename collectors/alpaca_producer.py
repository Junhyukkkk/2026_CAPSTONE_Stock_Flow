"""
Alpaca 실시간 주식 데이터 수집 및 Kafka 전송 Producer
안정성과 모니터링 기능 포함
"""
import asyncio
import json
import logging
import signal
import sys
import time
from datetime import datetime
from typing import List

import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from config import Config
from kafka_producer import KafkaProducerWrapper
from normalizer import DataNormalizer
from utils import ExponentialBackoff

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class AlpacaCollector:
    """Alpaca 데이터 수집 클래스"""
    
    def __init__(self):
        self.config = Config
        self.kafka_producer = KafkaProducerWrapper('alpaca')
        self.normalizer = DataNormalizer()
        self.backoff = ExponentialBackoff(
            initial_delay=self.config.RECONNECT_INITIAL_DELAY_SEC,
            max_delay=self.config.RECONNECT_MAX_DELAY_SEC,
            multiplier=self.config.RECONNECT_BACKOFF_MULTIPLIER
        )
        self.running = True
        self.websocket = None
        self._last_health_write = 0.0

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # API 키 검증
        if not self.config.ALPACA_API_KEY or not self.config.ALPACA_API_SECRET:
            raise ValueError(
                "ALPACA_API_KEY와 ALPACA_API_SECRET이 설정되어야 합니다"
            )

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"🛑 종료 시그널 수신 ({signum})")
        self.running = False
        if self.websocket:
            asyncio.create_task(self.websocket.close())

    def _update_health(self):
        """
        마지막 메시지 처리 성공 시각을 헬스체크 파일에 기록.
        (Docker HEALTHCHECK가 healthcheck.py로 이 파일을 읽어 판단)
        과도한 파일 I/O를 피하기 위해 최소 기록 간격을 둔다.
        """
        now = time.time()
        if now - self._last_health_write < self.config.HEALTH_WRITE_MIN_INTERVAL_SEC:
            return
        self._last_health_write = now
        try:
            with open(self.config.HEALTH_FILE_PATH, 'w') as f:
                json.dump({'last_success_epoch': now}, f)
        except OSError as e:
            logger.debug(f"헬스체크 파일 기록 실패: {e}")

    def get_subscribe_symbols(self) -> List[str]:
        """구독할 종목 리스트 반환"""
        if self.config.ALPACA_SUBSCRIBE_ALL_STOCKS:
            return ["*"]  # 전체 종목 구독
        
        if self.config.ALPACA_SUBSCRIBE_SYMBOLS:
            # 쉼표로 구분된 종목 리스트 파싱
            symbols = [
                s.strip().upper()
                for s in self.config.ALPACA_SUBSCRIBE_SYMBOLS.split(',')
                if s.strip()
            ]
            return symbols if symbols else ["*"]
        
        # 기본값: 전체 종목
        return ["*"]
    
    def build_auth_message(self) -> str:
        """인증 메시지 생성"""
        return json.dumps({
            "action": "auth",
            "key": self.config.ALPACA_API_KEY,
            "secret": self.config.ALPACA_API_SECRET
        })
    
    def build_subscribe_message(self) -> str:
        """구독 메시지 생성"""
        symbols = self.get_subscribe_symbols()
        # quotes는 수신해도 처리하지 않고 버리는데(핸들러가 pass), 플랜별 동시
        # 구독 한도를 trades와 같이 소모해 버려서 뺐다. quotes가 필요해지면
        # _process_single_message의 'q' 분기를 먼저 구현할 것.
        return json.dumps({
            "action": "subscribe",
            "trades": symbols,
        })
    
    async def send_message(self, websocket, message: str):
        """WebSocket 메시지 전송"""
        try:
            await websocket.send(message)
            logger.debug(f"📤 메시지 전송: {message[:100]}...")
        except Exception as e:
            logger.error(f"❌ 메시지 전송 실패: {e}")
            raise
    
    async def handle_message(self, message: str):
        """수신된 메시지 처리"""
        try:
            data = json.loads(message)
            
            # 배열 형식 처리
            if isinstance(data, list):
                for item in data:
                    # 배치 내 한 항목 처리가 실패해도 나머지 항목은 계속 처리한다
                    # (이전에는 예외가 바깥 try/except까지 전파되어 같은 배치의
                    # 나머지 메시지가 전부 유실됐다)
                    try:
                        await self._process_single_message(item)
                    except Exception as e:
                        logger.error(
                            f"메시지 처리 중 오류 (배치 내 해당 항목만 건너뜀): {e}, "
                            f"항목: {item}",
                            exc_info=True
                        )
            else:
                await self._process_single_message(data)
                
        except json.JSONDecodeError as e:
            logger.debug(f"JSON 파싱 실패: {e}, 메시지: {message[:200]}")
        except Exception as e:
            logger.error(f"메시지 처리 중 오류: {e}", exc_info=True)
    
    async def _process_single_message(self, message: dict):
        """단일 메시지 처리"""
        message_type = message.get('T', '')
        
        if message_type == 'success':
            msg = message.get('msg', '')
            if msg == 'authenticated':
                logger.info("✅ Alpaca 인증 성공")
            else:
                logger.info(f"✅ 성공 메시지: {msg}")
        
        elif message_type == 'error':
            code = message.get('code', -1)
            msg = message.get('msg', 'Unknown error')
            logger.error(f"❌ Alpaca 오류 (코드: {code}): {msg}")
        
        elif message_type == 't':  # Trade (체결)
            normalized = self.normalizer.normalize_alpaca_trade_data(message)
            if normalized:
                success = self.kafka_producer.produce(
                    topic=self.config.KAFKA_TOPIC_NAME,
                    key=normalized.symbol,
                    value=normalized.to_dict()
                )
                if success:
                    self._update_health()
                    self.kafka_producer.log_stats()

        elif message_type == 'q':  # Quote (호가)
            # Quote는 실제 체결이 아니므로 선택적으로 처리
            # 필요시 주석 해제하여 사용
            # normalized = self.normalizer.normalize_alpaca_quote_data(message)
            # if normalized:
            #     self.kafka_producer.produce(
            #         topic=self.config.KAFKA_TOPIC_NAME,
            #         key=normalized.symbol,
            #         value=normalized.to_dict()
            #     )
            pass
        
        elif message_type == 'subscription':
            trades = message.get('trades', [])
            quotes = message.get('quotes', [])
            logger.info(
                f"✅ 구독 완료 | 체결: {len(trades)}개 종목 | 호가: {len(quotes)}개 종목"
            )
        
        else:
            logger.debug(f"알 수 없는 메시지 타입: {message_type}")
    
    async def collect_data(self):
        """데이터 수집 메인 루프"""
        topic_name = self.config.KAFKA_TOPIC_NAME
        websocket_url = self.config.ALPACA_WEBSOCKET_URL
        
        symbols = self.get_subscribe_symbols()
        logger.info(
            f"🚀 Alpaca 데이터 수집 시작 | "
            f"구독: {symbols} | "
            f"Topic: {topic_name} | "
            f"URL: {websocket_url}"
        )
        
        # 무한 재연결 루프
        while self.running:
            try:
                logger.info(f"🔌 WebSocket 연결 시도: {websocket_url}")
                
                async with websockets.connect(
                    websocket_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10
                ) as websocket:
                    self.websocket = websocket
                    logger.info("✅ Alpaca WebSocket 연결 성공")
                    self.backoff.reset()  # 연결 성공 시 백오프 초기화
                    
                    # 인증 메시지 전송
                    auth_msg = self.build_auth_message()
                    await self.send_message(websocket, auth_msg)
                    logger.info("📤 인증 메시지 전송 완료")
                    
                    # 인증 응답 대기 (간단한 타임아웃)
                    try:
                        auth_response = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=5.0
                        )
                        await self.handle_message(auth_response)
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ 인증 응답 타임아웃 (계속 진행)")
                    
                    # 구독 메시지 전송
                    subscribe_msg = self.build_subscribe_message()
                    await self.send_message(websocket, subscribe_msg)
                    logger.info(f"📤 구독 메시지 전송 완료: {symbols}")
                    
                    # 메시지 수신 루프
                    async for message in websocket:
                        if not self.running:
                            break
                        await self.handle_message(message)
            
            except ConnectionClosed as e:
                if self.running:
                    logger.warning(
                        f"⚠️ WebSocket 연결 종료 (코드: {e.code}, 이유: {e.reason}). "
                        f"{self.backoff.current_delay:.1f}초 후 재연결..."
                    )
                    await self.backoff.async_wait()
            
            except InvalidStatus as e:
                logger.error(f"❌ WebSocket 연결 실패 (HTTP {e.response.status_code})")
                if self.running:
                    await self.backoff.async_wait()
            
            except Exception as e:
                if self.running:
                    logger.error(
                        f"❌ 예상치 못한 오류 발생: {e}",
                        exc_info=True
                    )
                    await self.backoff.async_wait()
            
            finally:
                self.websocket = None
        
        logger.info("🛑 데이터 수집 종료")
    
    def shutdown(self):
        """정상 종료 처리"""
        logger.info("🧹 종료 처리 시작...")
        self.running = False
        self.kafka_producer.close()
        
        # 최종 통계 출력
        metrics = self.kafka_producer.get_metrics()
        logger.info(
            f"📊 최종 통계 | "
            f"전송: {metrics['total_sent']:,}건 | "
            f"실패: {metrics['total_failed']:,}건 | "
            f"평균 속도: {metrics['messages_per_second']:.2f} msg/s | "
            f"성공률: {metrics['success_rate']*100:.2f}%"
        )
        logger.info("✅ 종료 완료")


async def main():
    """메인 함수"""
    # 설정 검증
    if not Config.validate():
        logger.error("❌ 설정 검증 실패. 종료합니다.")
        sys.exit(1)
    
    # Alpaca API 키 검증
    if not Config.ALPACA_API_KEY or not Config.ALPACA_API_SECRET:
        logger.error("❌ ALPACA_API_KEY와 ALPACA_API_SECRET이 필요합니다")
        sys.exit(1)
    
    collector = AlpacaCollector()
    
    try:
        await collector.collect_data()
    except KeyboardInterrupt:
        logger.info("🛑 사용자에 의한 종료")
    except Exception as e:
        logger.error(f"❌ 치명적 오류: {e}", exc_info=True)
        sys.exit(1)
    finally:
        collector.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 프로그램 종료")
    except Exception as e:
        logger.error(f"❌ 프로그램 실행 중 오류: {e}", exc_info=True)
        sys.exit(1)
