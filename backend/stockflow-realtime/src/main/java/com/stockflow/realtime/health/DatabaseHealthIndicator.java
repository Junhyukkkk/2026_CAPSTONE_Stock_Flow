package com.stockflow.realtime.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;

/**
 * 데이터베이스 헬스체크
 * 
 * PostgreSQL (TimescaleDB) 연결 상태 확인
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseHealthIndicator implements HealthIndicator {

    private final DataSource dataSource;

    @Override
    public Health health() {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            String databaseProductName = metaData.getDatabaseProductName();
            String databaseProductVersion = metaData.getDatabaseProductVersion();
            String url = metaData.getURL();
            
            return Health.up()
                .withDetail("database", databaseProductName)
                .withDetail("version", databaseProductVersion)
                .withDetail("url", url)
                .build();
                
        } catch (Exception e) {
            log.error("Database health check failed", e);
            return Health.down()
                .withDetail("error", e.getMessage())
                .build();
        }
    }
}
