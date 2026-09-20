"""
Binance 실시간 데이터 수집 및 Kafka 전송 Producer
안정성과 모니터링 기능 포함
"""
import asyncio
import json
import logging
import requests
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


class BinanceCollector:
    """Binance 데이터 수집 클래스"""
    
    def __init__(self):
        self.config = Config
        self.kafka_producer = KafkaProducerWrapper('binance')
        self.normalizer = DataNormalizer()
        self.backoff = ExponentialBackoff(
            initial_delay=self.config.RECONNECT_INITIAL_DELAY_SEC,
            max_delay=self.config.RECONNECT_MAX_DELAY_SEC,
            multiplier=self.config.RECONNECT_BACKOFF_MULTIPLIER
        )
        self.symbols: List[str] = []
        self.running = True
        self.last_symbol_refresh = datetime.utcnow()
        self._last_health_write = 0.0

        # 종료 시그널 핸들러
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """종료 시그널 처리"""
        logger.info(f"🛑 종료 시그널 수신 ({signum})")
        self.running = False

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

    def get_top_volume_symbols(self, limit: int = None) -> List[str]:
        """
        거래 중인 USDT 마켓 종목 리스트 조회

        Args:
            limit: 조회할 종목 수 상한 (기본값: 설정값 사용). 0 이하면 전체 종목 반환.

        Returns:
            종목 심볼 리스트 (소문자)
        """
        if limit is None:
            limit = self.config.BINANCE_TOP_SYMBOLS_LIMIT

        try:
            url = "https://api.binance.com/api/v3/ticker/24hr"
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            tickers = response.json()
            usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT')]
            usdt_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)

            if limit and limit > 0:
                usdt_tickers = usdt_tickers[:limit]

            symbols = [t['symbol'].lower() for t in usdt_tickers]
            logger.info(f"✅ {len(symbols)}개 종목 조회 완료 ({'상위 ' + str(limit) + '개' if limit and limit > 0 else '전체'})")
            return symbols
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ 종목 리스트 조회 실패: {e}")
            return []
        except (KeyError, ValueError, TypeError) as e:
            logger.error(f"❌ 종목 리스트 파싱 실패: {e}")
            return []
    
    def should_refresh_symbols(self) -> bool:
        """종목 리스트 갱신 필요 여부 확인"""
        if not self.symbols:
            return True
        
        elapsed_hours = (
            (datetime.utcnow() - self.last_symbol_refresh).total_seconds() / 3600
        )
        return elapsed_hours >= self.config.BINANCE_SYMBOL_REFRESH_INTERVAL_HOURS
    
    def refresh_symbols(self) -> bool:
        """종목 리스트 갱신"""
        new_symbols = self.get_top_volume_symbols()
        if new_symbols:
            old_count = len(self.symbols)
            self.symbols = new_symbols
            self.last_symbol_refresh = datetime.utcnow()
            
            if old_count != len(self.symbols):
                logger.info(
                    f"🔄 종목 리스트 갱신: {old_count}개 -> {len(self.symbols)}개"
                )
            return True
        return False
    
    def build_websocket_uri(self) -> str:
        """WebSocket URI 생성"""
        if not self.symbols:
            raise ValueError("종목 리스트가 비어있습니다")
        
        streams = "/".join([f"{s}@aggTrade" for s in self.symbols])
        uri = f"wss://stream.binance.com:9443/stream?streams={streams}"
        return uri
    
    async def collect_data(self):
        """데이터 수집 메인 루프"""
        topic_name = self.config.BINANCE_TOPIC_NAME
        
        # 초기 종목 리스트 조회 (실패 시 지수 백오프로 재시도.
        # Binance REST 엔드포인트를 즉시 재요청으로 두드려 레이트리밋/차단
        # 당하는 것을 방지한다 - 재연결 경로와 동일한 패턴)
        while self.running and not self.refresh_symbols():
            logger.warning(
                f"⚠️ 초기 종목 리스트 조회 실패. "
                f"{self.backoff.current_delay:.1f}초 후 재시도..."
            )
            await self.backoff.async_wait()

        if not self.running:
            logger.info("🛑 초기화 중 종료 시그널 수신. 종료합니다.")
            return

        if not self.symbols:
            logger.critical("❌ 초기 종목 리스트를 가져올 수 없습니다. 종료합니다.")
            return

        self.backoff.reset()  # 초기 조회 성공 후 백오프 초기화

        logger.info(
            f"🚀 Binance 데이터 수집 시작 | "
            f"종목: {len(self.symbols)}개 | "
            f"Topic: {topic_name}"
        )
        
        # 무한 재연결 루프
        while self.running:
            try:
                # 종목 리스트 갱신 확인
                if self.should_refresh_symbols():
                    if not self.refresh_symbols():
                        logger.warning("⚠️ 종목 리스트 갱신 실패. 기존 리스트 사용")
                
                # WebSocket 연결
                uri = self.build_websocket_uri()
                logger.info(f"🔌 WebSocket 연결 시도: {len(self.symbols)}개 스트림")
                
                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10
                ) as websocket:
                    logger.info("✅ Binance WebSocket 연결 성공")
                    self.backoff.reset()  # 연결 성공 시 백오프 초기화
                    
                    # 메시지 수신 루프
                    async for raw_message in websocket:
                        if not self.running:
                            break

                        # 종목 리스트 갱신 주기가 도래하면, 연결이 계속 살아있어도
                        # (should_refresh_symbols가 재연결 시점에만 체크되어 무한정
                        # 미뤄지는 것을 방지하기 위해) 스스로 연결을 종료해 바깥
                        # while 루프에서 갱신 후 재연결하도록 한다.
                        if self.should_refresh_symbols():
                            logger.info(
                                "🔄 종목 리스트 갱신 주기 도달. WebSocket을 재연결합니다."
                            )
                            break

                        try:
                            data = json.loads(raw_message)
                            normalized = self.normalizer.normalize_binance_data(data)

                            if normalized:
                                # Kafka 전송
                                success = self.kafka_producer.produce(
                                    topic=topic_name,
                                    key=normalized.symbol,
                                    value=normalized.to_dict()
                                )

                                if success:
                                    self._update_health()
                                    # 통계 로그 출력
                                    self.kafka_producer.log_stats()

                        except json.JSONDecodeError as e:
                            logger.debug(f"JSON 파싱 실패: {e}")
                        except Exception as e:
                            logger.error(f"메시지 처리 중 오류: {e}", exc_info=True)
            
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
    
    collector = BinanceCollector()
    
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
