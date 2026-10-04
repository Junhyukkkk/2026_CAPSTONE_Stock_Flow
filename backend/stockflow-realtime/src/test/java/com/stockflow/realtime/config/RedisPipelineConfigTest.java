package com.stockflow.realtime.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.redis.connection.lettuce.LettuceConnection.PipeliningFlushPolicy;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisPipelineConfigTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(RedisPipelineConfig.class)
        .withBean(PropertySourcesPlaceholderConfigurer.class, PropertySourcesPlaceholderConfigurer::new)
        .withBean(LettuceConnectionFactory.class, () -> new LettuceConnectionFactory());

    private static Object policyOf(LettuceConnectionFactory factory) {
        return ReflectionTestUtils.getField(factory, "pipeliningFlushPolicy");
    }

    @Test
    void parse_each_leavesFactoryUntouched() {
        assertThat(RedisPipelineConfig.parse("each")).isEmpty();
    }

    @Test
    void parse_close() {
        assertThat(RedisPipelineConfig.parse("close")).containsSame(PipeliningFlushPolicy.flushOnClose());
    }

    @Test
    void parse_buffered() {
        assertThat(RedisPipelineConfig.parse("buffered:100")).isPresent();
    }

    @Test
    void parse_invalid_throwsWithClearMessage() {
        for (String bad : new String[] {"", "foo", "buffered", "buffered:", "buffered:abc", "buffered:0", "buffered:-5"}) {
            assertThatThrownBy(() -> RedisPipelineConfig.parse(bad))
                .as(bad)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("REDIS_PIPELINE_FLUSH");
        }
    }

    @Test
    void default_doesNotChangeFactoryPolicy() {
        Object untouched = policyOf(new LettuceConnectionFactory());

        runner.run(ctx -> assertThat(policyOf(ctx.getBean(LettuceConnectionFactory.class))).isSameAs(untouched));
    }

    @Test
    void each_doesNotChangeFactoryPolicy() {
        Object untouched = policyOf(new LettuceConnectionFactory());

        runner.withPropertyValues("stockflow.redis.pipeline-flush=each")
            .run(ctx -> assertThat(policyOf(ctx.getBean(LettuceConnectionFactory.class))).isSameAs(untouched));
    }

    @Test
    void close_appliesFlushOnClose() {
        runner.withPropertyValues("stockflow.redis.pipeline-flush=close")
            .run(ctx -> assertThat(policyOf(ctx.getBean(LettuceConnectionFactory.class)))
                .isSameAs(PipeliningFlushPolicy.flushOnClose()));
    }

    @Test
    void envVarName_isHonoredWhenPropertyAbsent() {
        runner.withPropertyValues("REDIS_PIPELINE_FLUSH=close")
            .run(ctx -> assertThat(policyOf(ctx.getBean(LettuceConnectionFactory.class)))
                .isSameAs(PipeliningFlushPolicy.flushOnClose()));
    }

    @Test
    void buffered_appliesPolicyDifferentFromDefault() {
        Object untouched = policyOf(new LettuceConnectionFactory());

        runner.withPropertyValues("stockflow.redis.pipeline-flush=buffered:100")
            .run(ctx -> assertThat(policyOf(ctx.getBean(LettuceConnectionFactory.class)))
                .isNotNull().isNotSameAs(untouched));
    }

    @Test
    void invalidValue_failsStartup() {
        runner.withPropertyValues("stockflow.redis.pipeline-flush=bogus")
            .run(ctx -> assertThat(ctx).hasFailed());
    }
}
