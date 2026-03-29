package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 멱등 처리 (Consumer 종류별로 Redis 키 분리)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final RedisTemplate<String, String> redisTemplate;

    private String generateKey(String channel, NormalizedTradeDTO trade) {
        return String.format("processed:%s:%s:%s", channel, trade.getSource(), trade.getTradeId());
    }

    public boolean isAlreadyProcessed(String channel, NormalizedTradeDTO trade) {
        String key = generateKey(channel, trade);
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    public void markAsProcessed(String channel, NormalizedTradeDTO trade, long ttl) {
        String key = generateKey(channel, trade);
        redisTemplate.opsForValue().set(key, "1", ttl, TimeUnit.SECONDS);
        log.trace("Marked as processed: channel={}, key={}", channel, key);
    }

    public void markAsProcessed(String channel, NormalizedTradeDTO trade) {
        markAsProcessed(channel, trade, 86400);
    }

    public void markBatchAsProcessed(String channel, List<NormalizedTradeDTO> trades) {
        for (NormalizedTradeDTO trade : trades) {
            markAsProcessed(channel, trade);
        }
        log.debug("Marked batch as processed: channel={}, size={}", channel, trades.size());
    }

    public void removeProcessedMark(String channel, NormalizedTradeDTO trade) {
        String key = generateKey(channel, trade);
        redisTemplate.delete(key);
        log.trace("Removed processed mark: channel={}, key={}", channel, key);
    }
}
