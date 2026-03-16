package com.stockflow.realtime.config;

import com.stockflow.realtime.listener.RedisMessageListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Redis 설정
 *
 * - RedisTemplate: 캐싱 및 Pub/Sub 발행
 * - MessageListenerContainer: Pub/Sub 구독
 */
@Configuration
public class RedisConfig {

    /**
     * RedisTemplate 설정
     *
     * String 타입의 키/값 저장용
     */
    @Bean
    public RedisTemplate<String, String> redisTemplate(RedisConnectionFactory connectionFactory) {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // String 직렬화
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(new StringRedisSerializer());

        template.afterPropertiesSet();
        return template;
    }

    /**
     * Redis Pub/Sub 구독 설정
     *
     * price:* 패턴의 모든 채널을 구독하여
     * RedisMessageListener로 전달
     */
    @Bean
    public RedisMessageListenerContainer redisMessageListenerContainer(
            RedisConnectionFactory connectionFactory,
            RedisMessageListener redisMessageListener) {

        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);

        // price:* 패턴 구독 (price:AAPL, price:BTCUSDT 등)
        container.addMessageListener(redisMessageListener, new PatternTopic("price:*"));

        return container;
    }
}
