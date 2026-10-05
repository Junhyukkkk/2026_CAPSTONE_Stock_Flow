package com.stockflow.realtime.config;

import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceConnection.PipeliningFlushPolicy;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Optional;

/**
 * Lettuce 파이프라인 flush 정책 설정 (env REDIS_PIPELINE_FLUSH)
 *
 * - each (기본): 명령마다 flush — 팩토리를 건드리지 않아 기존 동작 그대로
 * - close: 파이프라인 close 시 한 번에 flush
 * - buffered:N: N개 명령마다 flush (예: buffered:100)
 */
@Configuration
public class RedisPipelineConfig {

    // Boot 가 자동 구성한 팩토리를 초기화 전에 가로채 정책만 바꾼다 (새 팩토리를 만들지 않는다)
    @Bean
    public static BeanPostProcessor redisPipelineFlushPolicyPostProcessor(
            @Value("${stockflow.redis.pipeline-flush:${REDIS_PIPELINE_FLUSH:each}}") String spec) {
        Optional<PipeliningFlushPolicy> policy = parse(spec);
        return new BeanPostProcessor() {
            @Override
            public Object postProcessBeforeInitialization(Object bean, String beanName) {
                if (bean instanceof LettuceConnectionFactory factory) {
                    policy.ifPresent(factory::setPipeliningFlushPolicy);
                }
                return bean;
            }
        };
    }

    /** each 는 empty(= 정책 미변경), 알 수 없는 값은 IllegalArgumentException. */
    static Optional<PipeliningFlushPolicy> parse(String spec) {
        String value = spec == null ? "" : spec.trim();
        if (value.equals("each")) {
            return Optional.empty();
        }
        if (value.equals("close")) {
            return Optional.of(PipeliningFlushPolicy.flushOnClose());
        }
        if (value.startsWith("buffered:")) {
            try {
                int n = Integer.parseInt(value.substring("buffered:".length()).trim());
                if (n > 0) {
                    return Optional.of(PipeliningFlushPolicy.buffered(n));
                }
            } catch (NumberFormatException ignored) {
                // 아래 공통 예외로 처리
            }
        }
        throw new IllegalArgumentException(
                "Invalid stockflow.redis.pipeline-flush (REDIS_PIPELINE_FLUSH): '" + spec
                        + "' — expected 'each', 'close' or 'buffered:<n>' (n > 0)");
    }
}
