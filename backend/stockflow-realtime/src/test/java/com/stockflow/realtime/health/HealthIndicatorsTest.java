package com.stockflow.realtime.health;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class HealthIndicatorsTest {

    // ----- Database -----

    @Test
    void databaseUpReportsProductDetails() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getDatabaseProductName()).thenReturn("PostgreSQL");
        when(metaData.getDatabaseProductVersion()).thenReturn("16");
        when(metaData.getURL()).thenReturn("jdbc:postgresql://db/stockflow");

        Health health = new DatabaseHealthIndicator(dataSource).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("database", "PostgreSQL").containsEntry("version", "16");
    }

    @Test
    void databaseDownWhenConnectionFails() throws SQLException {
        DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenThrow(new SQLException("refused"));

        Health health = new DatabaseHealthIndicator(dataSource).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "refused");
    }

    // ----- Redis -----

    private RedisTemplate<String, String> redisReturningPing(String pong) {
        RedisTemplate<String, String> template = mock(RedisTemplate.class);
        RedisConnectionFactory factory = mock(RedisConnectionFactory.class);
        RedisConnection connection = mock(RedisConnection.class);
        when(template.getConnectionFactory()).thenReturn(factory);
        when(factory.getConnection()).thenReturn(connection);
        when(connection.ping()).thenReturn(pong);
        return template;
    }

    @Test
    void redisUpOnPong() {
        assertThat(new RedisHealthIndicator(redisReturningPing("PONG")).health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void redisDownOnUnexpectedReply() {
        Health health = new RedisHealthIndicator(redisReturningPing("NOPE")).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails().get("status").toString()).contains("NOPE");
    }

    @Test
    void redisDownWhenConnectionThrows() {
        RedisTemplate<String, String> template = mock(RedisTemplate.class);
        when(template.getConnectionFactory()).thenThrow(new IllegalStateException("no factory"));

        Health health = new RedisHealthIndicator(template).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "no factory");
    }
}
