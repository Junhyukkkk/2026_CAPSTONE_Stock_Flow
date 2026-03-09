"""
Kafka Producer 래퍼 모듈
에러 처리, 메트릭, 재시도 로직 포함
"""
import json
import logging
import time
from typing import Optional, Dict, Any
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from config import Config

logger = logging.getLogger(__name__)


class KafkaProducerWrapper:
    """Kafka Producer 래퍼 클래스"""
    
    def __init__(self, client_id_suffix: str):
        self.client_id_suffix = client_id_suffix
        self.producer: Optional[Producer] = None
        self.dlq_producer: Optional[Producer] = None
        self.metrics = {
            'total_sent': 0,
            'total_failed': 0,
            'total_errors': 0,
            'total_dlq_sent': 0,
            'last_error': None,
            'last_error_time': None,
            'start_time': time.time()
        }
        self._initialize_producer()
        if Config.DLQ_ENABLED:
            self._initialize_dlq_producer()
    
    def _initialize_producer(self):
        """Producer 초기화"""
        try:
            conf = Config.get_kafka_producer_config(self.client_id_suffix)
            self.producer = Producer(conf)
            logger.info(f"✅ Kafka Producer 초기화 완료: {conf['client.id']}")
        except Exception as e:
            logger.error(f"❌ Kafka Producer 초기화 실패: {e}")
            raise
    
    def _initialize_dlq_producer(self):
        """DLQ Producer 초기화"""
        try:
            conf = Config.get_kafka_producer_config(f"{self.client_id_suffix}-dlq")
            self.dlq_producer = Producer(conf)
            logger.info(f"✅ DLQ Producer 초기화 완료: {conf['client.id']}")
        except Exception as e:
            logger.warning(f"⚠️ DLQ Producer 초기화 실패: {e} (DLQ 기능 비활성화)")
            self.dlq_producer = None
    
    def _delivery_report(self, err, msg):
        """메시지 전송 결과 콜백"""
        if err is not None:
            self.metrics['total_failed'] += 1
            self.metrics['total_errors'] += 1
            self.metrics['last_error'] = str(err)
            self.metrics['last_error_time'] = time.time()
            logger.error(f"❌ Kafka 전송 실패: {err}, 메시지: {msg}")
            
            # DLQ로 전송 시도
            if Config.DLQ_ENABLED and msg and self.dlq_producer:
                self._send_to_dlq(msg, str(err))
        else:
            self.metrics['total_sent'] += 1
    
    def _send_to_dlq(self, original_msg, error: str):
        """실패한 메시지를 DLQ로 전송"""
        if not self.dlq_producer:
            return
        
        try:
            # DLQ 메시지 구조: 원본 메시지 + 에러 정보
            dlq_message = {
                'original_topic': original_msg.topic() if hasattr(original_msg, 'topic') else 'unknown',
                'original_key': original_msg.key().decode('utf-8') if original_msg.key() else None,
                'original_value': original_msg.value().decode('utf-8') if original_msg.value() else None,
                'error': error,
                'timestamp': int(time.time() * 1000),
                'source': self.client_id_suffix
            }
            
            dlq_value = json.dumps(dlq_message)
            
            self.dlq_producer.produce(
                topic=Config.DLQ_TOPIC_NAME,
                key=original_msg.key() if original_msg.key() else 'unknown',
                value=dlq_value,
                callback=lambda err, msg: (
                    self._dlq_delivery_report(err, msg)
                )
            )
            self.dlq_producer.poll(0)
            self.metrics['total_dlq_sent'] += 1
            
        except Exception as e:
            logger.error(f"❌ DLQ 전송 실패: {e}")
    
    def _dlq_delivery_report(self, err, msg):
        """DLQ 전송 결과 콜백"""
        if err is not None:
            logger.error(f"❌ DLQ 전송 실패: {err}")
        else:
            logger.debug(f"✅ DLQ 전송 성공: {msg.topic()}")
    
    def produce(self, topic: str, key: str, value: Dict[str, Any], callback=None):
        """
        메시지 전송
        
        Args:
            topic: Kafka 토픽 이름
            key: 메시지 키 (보통 symbol)
            value: 메시지 값 (NormalizedTradeDTO 딕셔너리)
            callback: 추가 콜백 함수 (선택사항)
        """
        if not self.producer:
            logger.error("Producer가 초기화되지 않았습니다")
            return False
        
        try:
            # JSON 직렬화
            value_json = json.dumps(value)
            
            # 메시지 전송
            self.producer.produce(
                topic=topic,
                key=key,
                value=value_json,
                callback=lambda err, msg: (
                    self._delivery_report(err, msg),
                    callback(err, msg) if callback else None
                )
            )
            
            # Non-blocking poll (백그라운드 전송 처리)
            self.producer.poll(0)
            
            return True
            
        except BufferError:
            # 큐가 가득 찬 경우
            logger.warning("⚠️ Kafka Producer 큐가 가득 참. flush 시도...")
            self.producer.poll(0.1)
            # DLQ로 전송 시도
            if Config.DLQ_ENABLED and self.dlq_producer:
                self._send_to_dlq_directly(key, value, "BufferError: 큐가 가득 참")
            return False
        except Exception as e:
            self.metrics['total_errors'] += 1
            logger.error(f"❌ 메시지 전송 중 예외 발생: {e}")
            # DLQ로 전송 시도
            if Config.DLQ_ENABLED and self.dlq_producer:
                self._send_to_dlq_directly(key, value, str(e))
            return False
    
    def _send_to_dlq_directly(self, key: str, value: Dict[str, Any], error: str):
        """직렬화 전 단계에서 실패한 경우 DLQ로 전송"""
        if not self.dlq_producer:
            return
        
        try:
            dlq_message = {
                'original_key': key,
                'original_value': value,
                'error': error,
                'timestamp': int(time.time() * 1000),
                'source': self.client_id_suffix,
                'note': '직렬화 전 단계에서 실패'
            }
            
            dlq_value = json.dumps(dlq_message)
            
            self.dlq_producer.produce(
                topic=Config.DLQ_TOPIC_NAME,
                key=key or 'unknown',
                value=dlq_value,
                callback=lambda err, msg: (
                    self._dlq_delivery_report(err, msg)
                )
            )
            self.dlq_producer.poll(0)
            self.metrics['total_dlq_sent'] += 1
            
        except Exception as e:
            logger.error(f"❌ DLQ 직접 전송 실패: {e}")
    
    def flush(self, timeout: float = 5.0):
        """대기 중인 모든 메시지 전송 완료 대기"""
        if self.producer:
            try:
                remaining = self.producer.flush(timeout)
                if remaining > 0:
                    logger.warning(f"⚠️ {remaining}개 메시지가 전송되지 않았습니다")
                else:
                    logger.info("✅ 모든 메시지 전송 완료")
            except Exception as e:
                logger.error(f"❌ Flush 중 오류 발생: {e}")
        
        if self.dlq_producer:
            try:
                self.dlq_producer.flush(timeout)
            except Exception as e:
                logger.error(f"❌ DLQ Flush 중 오류 발생: {e}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """현재 메트릭 반환"""
        elapsed = time.time() - self.metrics['start_time']
        return {
            **self.metrics,
            'elapsed_seconds': elapsed,
            'messages_per_second': self.metrics['total_sent'] / elapsed if elapsed > 0 else 0,
            'success_rate': (
                (self.metrics['total_sent'] / (self.metrics['total_sent'] + self.metrics['total_failed']))
                if (self.metrics['total_sent'] + self.metrics['total_failed']) > 0 else 0
            )
        }
    
    def log_stats(self, interval: int = None):
        """통계 로그 출력"""
        if interval is None:
            interval = Config.LOG_STATS_INTERVAL
        
        if self.metrics['total_sent'] % interval == 0 and self.metrics['total_sent'] > 0:
            metrics = self.get_metrics()
            dlq_info = f" | DLQ: {metrics.get('total_dlq_sent', 0):,}건" if Config.DLQ_ENABLED else ""
            logger.info(
                f"📊 통계 | 전송: {metrics['total_sent']:,}건 | "
                f"실패: {metrics['total_failed']:,}건{dlq_info} | "
                f"속도: {metrics['messages_per_second']:.2f} msg/s | "
                f"성공률: {metrics['success_rate']*100:.2f}%"
            )
    
    def close(self):
        """Producer 종료"""
        if self.producer:
            self.flush()
            logger.info("✅ Kafka Producer 종료")
