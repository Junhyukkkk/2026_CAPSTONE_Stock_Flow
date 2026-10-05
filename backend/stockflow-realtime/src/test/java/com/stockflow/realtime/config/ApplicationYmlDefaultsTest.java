package com.stockflow.realtime.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 운영 application.yml 의 검증된 기본값(Lettuce 풀 / JDBC URL / Hikari 풀)과
 * 환경변수(relaxed binding)로 옛 값을 지정했을 때의 롤백 경로를 검증한다.
 */
class ApplicationYmlDefaultsTest {

    private static StandardEnvironment environment(Map<String, Object> envVars) throws IOException {
        StandardEnvironment env = new StandardEnvironment();
        env.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        env.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        env.getPropertySources().addLast(
            new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, envVars));

        // 운영 application.yml (test 리소스의 application-test.yml 과는 별개 파일)
        List<PropertySource<?>> yaml = new YamlPropertySourceLoader()
            .load("application", new ClassPathResource("application.yml"));
        yaml.forEach(env.getPropertySources()::addLast);
        ConfigurationPropertySources.attach(env);
        return env;
    }

    @Test
    void provenDefaultsAreLoaded() throws IOException {
        StandardEnvironment env = environment(Map.of());
        Binder binder = Binder.get(env);

        assertThat(binder.bind("spring.data.redis.lettuce.pool.max-active", Integer.class).get()).isEqualTo(48);
        assertThat(binder.bind("spring.datasource.hikari.maximum-pool-size", Integer.class).get()).isEqualTo(24);
        assertThat(env.getProperty("spring.datasource.url"))
            .isEqualTo("jdbc:postgresql://localhost:5433/stockflow?reWriteBatchedInserts=true");
    }

    @Test
    void dbEnvStillAssemblesUrlAndKeepsRewriteFlag() throws IOException {
        StandardEnvironment env = environment(Map.of(
            "DB_HOST", "timescaledb", "DB_PORT", "5432", "DB_NAME", "stockflow"));

        assertThat(env.getProperty("spring.datasource.url"))
            .isEqualTo("jdbc:postgresql://timescaledb:5432/stockflow?reWriteBatchedInserts=true");
    }

    @Test
    void relaxedBindingEnvVarsRestoreOldPoolSizes() throws IOException {
        StandardEnvironment env = environment(Map.of(
            "SPRING_DATA_REDIS_LETTUCE_POOL_MAX_ACTIVE", "16",
            "SPRING_DATASOURCE_HIKARI_MAXIMUM_POOL_SIZE", "10"));
        Binder binder = Binder.get(env);

        assertThat(binder.bind("spring.data.redis.lettuce.pool.max-active", Integer.class).get()).isEqualTo(16);
        assertThat(binder.bind("spring.datasource.hikari.maximum-pool-size", Integer.class).get()).isEqualTo(10);
    }

    @Test
    void redisPasswordIsEmptyWithoutEnvAndResolvesFromEnv() throws IOException {
        assertThat(environment(Map.of()).getProperty("spring.data.redis.password")).isEmpty();
        assertThat(environment(Map.of("REDIS_PASSWORD", "s3cr3t")).getProperty("spring.data.redis.password"))
            .isEqualTo("s3cr3t");
    }
}
