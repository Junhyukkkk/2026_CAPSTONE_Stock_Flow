import asyncio
import websockets
import json
import requests
import logging
from datetime import datetime
from confluent_kafka import Producer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 카프카 설정
conf = {
    'bootstrap.servers': 'kafka:9093', 
    'client.id': 'binance-producer-v1',
    'linger.ms': 10,
    'batch.num.messages': 1000,
    'queue.buffering.max.messages': 200000,
    'compression.type': 'snappy',
    'acks': 1,
    'retries': 5,
    'request.timeout.ms': 30000
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"❌ Kafka 전송 실패: {err}")

def get_top_volume_symbols(limit=300):
    try:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        response = requests.get(url, timeout=10)
        tickers = response.json()
        usdt_tickers = [t for t in tickers if t['symbol'].endswith('USDT')]
        usdt_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        return [t['symbol'].lower() for t in usdt_tickers[:limit]]
    except Exception as e:
        logger.error(f"❌ 종목 리스트 갱신 실패: {e}")
        return []

def normalize_binance_data(json_data):
    raw = json_data.get('data', json_data)
    try:
        price = float(raw.get('p', 0))
        quantity = float(raw.get('q', 0))
        if price <= 0 or quantity <= 0: return None
        
        return {
            "standard_time": datetime.fromtimestamp(raw['T'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            "symbol": raw['s'],
            "price": format(price, '.8f').rstrip('0').rstrip('.'), 
            "quantity": format(quantity, '.8f').rstrip('0').rstrip('.'),
            "exchange": "BINANCE",
            "raw_timestamp": raw['T']
        }
    except: return None

async def binance_collector():
    topic_name = "market.binance.tick"
    
    # 1시간마다 종목 리스트를 갱신하도록 설계 가능 (일단 초기화)
    symbols = get_top_volume_symbols(limit=300)
    if not symbols:
        logger.critical("종목 리스트를 가져올 수 없습니다. 종료합니다.")
        return

    streams = "/".join([f"{s}@aggTrade" for s in symbols])
    uri = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    total_sent = 0
    logger.info(f"🚀 {len(symbols)}개 종목 수집 시작 (Topic: {topic_name})")

    # 재연결 로직: 네트워크 장애 시 무한 재시도
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=20) as websocket:
                logger.info("✅ 바이낸스 웹소켓 연결 성공")
                while True:
                    raw_message = await websocket.recv()
                    data = json.loads(raw_message)
                    refined_data = normalize_binance_data(data)
                    
                    if refined_data:
                        producer.produce(
                            topic=topic_name, 
                            key=refined_data['symbol'],
                            value=json.dumps(refined_data), 
                            callback=delivery_report
                        )
                        producer.poll(0) 
                    
                        total_sent += 1
                        if total_sent % 5000 == 0:
                            logger.info(f"📡 누적 {total_sent}개 Kafka 전송 중...")
        except websockets.ConnectionClosed:
            logger.warning("⚠️ 웹소켓 연결 종료. 5초 후 재시도합니다...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ 실행 중 에러 발생: {e}. 10초 후 재시도...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        asyncio.run(binance_collector())
    except KeyboardInterrupt:
        logger.info("🛑 사용자에 의한 종료")
    finally:
        logger.info("🧹 잔여 메시지 처리 중...")
        producer.flush(5)
        logger.info("✅ 시스템 정상 종료")