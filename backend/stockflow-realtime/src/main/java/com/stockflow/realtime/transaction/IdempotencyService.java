package com.stockflow.realtime.transaction;

import com.stockflow.core.dto.NormalizedTradeDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Idempotency 서비스
 * 
 * 중복 메시지 처리 방지
 * Redis를 사용하여 처리된 메시지 추적
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final RedisTemplate<String, String> redisTemplate;

    /**
     * 메시지 ID 키 생성
     * 
     * @param trade 거래 데이터
     * @return Redis 키
     */
    private String generateKey(NormalizedTradeDTO trade) {
        // source + tradeId로 고유 키 생성
        return String.format("processed:%s:%s", trade.getSource(), trade.getTradeId());
    }

    /**
     * 메시지가 이미 처리되었는지 확인
     * 
     * @param trade 거래 데이터
     * @return 이미 처리된 경우 true
     */
    public boolean isAlreadyProcessed(NormalizedTradeDTO trade) {
        String key = generateKey(trade);
        Boolean exists = redisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * 메시지 처리 완료 표시
     * 
     * @param trade 거래 데이터
     * @param ttl TTL (초) - 기본 24시간
     */
    public void markAsProcessed(NormalizedTradeDTO trade, long ttl) {
        String key = generateKey(trade);
        redisTemplate.opsForValue().set(key, "1", ttl, TimeUnit.SECONDS);
        log.trace("Marked as processed: key={}", key);
    }

    /**
     * 메시지 처리 완료 표시 (기본 TTL 사용)
     * 
     * @param trade 거래 데이터
     */
    public void markAsProcessed(NormalizedTradeDTO trade) {
        // 기본 TTL: 24시간 (86400초)
        markAsProcessed(trade, 86400);
    }

    /**
     * 배치 메시지 처리 완료 표시
     * 
     * @param trades 거래 데이터 리스트
     */
    public void markBatchAsProcessed(java.util.List<NormalizedTradeDTO> trades) {
        for (NormalizedTradeDTO trade : trades) {
            markAsProcessed(trade);
        }
        log.debug("Marked batch as processed: size={}", trades.size());
    }

    /**
     * 처리 완료 표시 제거 (테스트/디버깅용)
     * 
     * @param trade 거래 데이터
     */
    public void removeProcessedMark(NormalizedTradeDTO trade) {
        String key = generateKey(trade);
        redisTemplate.delete(key);
        log.trace("Removed processed mark: key={}", key);
    }
}
