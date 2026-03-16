package com.stockflow.realtime.consumer;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class RealtimeKafkaConsumer {

    // TODO: PriceCacheService 주입 (Redis 캐싱)
    // TODO: StringRedisTemplate 주입 (Pub/Sub)

    @KafkaListener(
            topics = "market.normalized",
            groupId = "realtime-consumer-group",
            containerFactory = "realtimeKafkaListenerContainerFactory"
    )
    public void onMessage(NormalizedTradeDTO trade) {
        try {
            log.debug("[Realtime] Received: {} - {} @ {}",
                    trade.getSymbol(),
                    trade.getPrice(),
                    trade.getTimestamp());

            // TODO: Redis 캐시 업데이트 (최신가 + 등락률)
            // PriceSnapshot snapshot = priceCacheService.updateLatestPrice(trade);

            // TODO: Redis Pub/Sub 발행
            // String channel = "price:" + trade.getSymbol();
            // redisTemplate.convertAndSend(channel, payload);

        } catch (Exception e) {
            log.error("[Realtime] Processing failed for {}: {}",
                    trade.getSymbol(), e.getMessage(), e);
            // TODO: DLQ 전송 로직 추가
        }
    }
}
