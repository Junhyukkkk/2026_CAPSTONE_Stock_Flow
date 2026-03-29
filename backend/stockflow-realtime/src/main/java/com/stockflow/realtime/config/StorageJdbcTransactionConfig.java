package com.stockflow.realtime.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

/**
 * Storage 배치 INSERT 전용 JDBC 트랜잭션.
 * JpaTransactionManager와 같은 경로에서 JdbcTemplate을 쓰면 "Unable to commit against JDBC Connection"이 날 수 있어 분리한다.
 */
@Configuration
public class StorageJdbcTransactionConfig {

    @Bean(name = "storageTransactionManager")
    public DataSourceTransactionManager storageTransactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean(name = "storageJdbcTransactionTemplate")
    public TransactionTemplate storageJdbcTransactionTemplate(
            @Qualifier("storageTransactionManager") DataSourceTransactionManager storageTransactionManager) {
        return new TransactionTemplate(storageTransactionManager);
    }
}
