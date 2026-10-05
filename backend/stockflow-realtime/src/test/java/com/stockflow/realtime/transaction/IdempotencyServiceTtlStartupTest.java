package com.stockflow.realtime.transaction;

import com.stockflow.realtime.config.OptimizationProperties;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * 실제 스프링 컨텍스트 기동 시 TTL 프로퍼티 바인딩과 검증(0 이하면 기동 실패)을 확인한다.
 */
class IdempotencyServiceTtlStartupTest {

    @SuppressWarnings("unchecked")
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withBean(RedisTemplate.class, () -> mock(RedisTemplate.class))
        .withBean(OptimizationProperties.class, OptimizationProperties::new)
        .withUserConfiguration(IdempotencyService.class);

    @Test
    void startsWithDefaultTtl600() {
        runner.run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ReflectionTestUtils.getField(ctx.getBean(IdempotencyService.class), "ttlSeconds"))
                .isEqualTo(600L);
        });
    }

    @Test
    void oldTtlFromPropertyRestoresOldBehavior() {
        runner.withPropertyValues("stockflow.idempotency.ttl-seconds=86400").run(ctx -> {
            assertThat(ctx).hasNotFailed();
            assertThat(ReflectionTestUtils.getField(ctx.getBean(IdempotencyService.class), "ttlSeconds"))
                .isEqualTo(86400L);
        });
    }

    @Test
    void zeroTtlFailsStartup() {
        runner.withPropertyValues("stockflow.idempotency.ttl-seconds=0").run(ctx ->
            assertThat(ctx).hasFailed().getFailure()
                .hasRootCauseInstanceOf(IllegalArgumentException.class)
                .hasStackTraceContaining("must be > 0"));
    }

    @Test
    void negativeTtlFailsStartup() {
        runner.withPropertyValues("stockflow.idempotency.ttl-seconds=-5").run(ctx ->
            assertThat(ctx).hasFailed().getFailure()
                .hasRootCauseInstanceOf(IllegalArgumentException.class));
    }
}
