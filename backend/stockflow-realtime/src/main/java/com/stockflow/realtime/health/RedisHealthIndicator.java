package com.stockflow.realtime.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Redis 헬스체크
 * 
 * Redis 연결 상태 확인
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RedisHealthIndicator implements HealthIndicator {

    private final RedisTemplate<String, String> redisTemplate;

    @Override
    public Health health() {
        try {
            // PING 명령으로 연결 확인
            String result = redisTemplate.getConnectionFactory()
                .getConnection()
                .ping();
            
            if ("PONG".equals(result)) {
                return Health.up()
                    .withDetail("status", "connected")
                    .build();
            } else {
                return Health.down()
                    .withDetail("status", "unexpected response: " + result)
                    .build();
            }
            
        } catch (Exception e) {
            log.error("Redis health check failed", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
