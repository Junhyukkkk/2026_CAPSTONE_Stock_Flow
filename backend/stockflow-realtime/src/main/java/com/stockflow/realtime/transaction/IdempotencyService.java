package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import com.stockflow.realtime.config.OptimizationProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 멱등 처리 (Consumer 종류별로 Redis 키 분리)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final long DEFAULT_TTL_SECONDS = 86400; // 24시간

    private final RedisTemplate<String, String> redisTemplate;
    private final OptimizationProperties opt;

    /**
     * DB 유니크 키 (symbol, source, trade_id, ts) 와 동일 축 — trade_ts는 epoch ms.
     */
    private String generateKey(String channel, NormalizedTradeDTO trade) {
        return String.format(
                "processed:%s:%s:%s:%s:%d",
                channel,
                trade.getSymbol(),
                trade.getSource(),
                trade.getTradeId(),
                trade.getTimestamp()
        );
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
        markAsProcessed(channel, trade, DEFAULT_TTL_SECONDS);
    }

    /**
     * 배치 멱등성 조회
     *
     * 기본 경로는 메시지당 EXISTS 1회 왕복이라 배치 크기만큼 왕복이 발생한다.
     * 파이프라인을 켜면 배치 전체를 1회 왕복으로 처리한다.
     *
     * @return trades 와 같은 순서의 "이미 처리됨" 여부
     */
    public List<Boolean> areAlreadyProcessed(String channel, List<NormalizedTradeDTO> trades) {
        if (trades.isEmpty()) {
            return List.of();
        }

        if (!opt.isStorageIdempotencyPipeline()) {
            List<Boolean> results = new ArrayList<>(trades.size());
            for (NormalizedTradeDTO trade : trades) {
                results.add(isAlreadyProcessed(channel, trade));
            }
            return results;
        }

        List<byte[]> keys = trades.stream()
                .map(t -> generateKey(channel, t).getBytes(StandardCharsets.UTF_8))
                .toList();

        List<Object> raw = redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (byte[] key : keys) {
                connection.keyCommands().exists(key);
            }
            return null;
        });

        List<Boolean> results = new ArrayList<>(trades.size());
        for (int i = 0; i < trades.size(); i++) {
            Object r = i < raw.size() ? raw.get(i) : null;
            results.add(Boolean.TRUE.equals(r));
        }
        return results;
    }

    public void markBatchAsProcessed(String channel, List<NormalizedTradeDTO> trades) {
        if (trades.isEmpty()) {
            return;
        }

        if (opt.isStorageIdempotencyPipeline()) {
            Expiration ttl = Expiration.seconds(DEFAULT_TTL_SECONDS);
            byte[] one = "1".getBytes(StandardCharsets.UTF_8);
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                for (NormalizedTradeDTO trade : trades) {
                    connection.stringCommands().set(
                            generateKey(channel, trade).getBytes(StandardCharsets.UTF_8),
                            one, ttl, RedisStringCommands.SetOption.upsert());
                }
                return null;
            });
        } else {
            for (NormalizedTradeDTO trade : trades) {
                markAsProcessed(channel, trade);
            }
        }
        log.debug("Marked batch as processed: channel={}, size={}", channel, trades.size());
    }

    public void removeProcessedMark(String channel, NormalizedTradeDTO trade) {
        String key = generateKey(channel, trade);
        redisTemplate.delete(key);
        log.trace("Removed processed mark: channel={}, key={}", channel, key);
    }
}
