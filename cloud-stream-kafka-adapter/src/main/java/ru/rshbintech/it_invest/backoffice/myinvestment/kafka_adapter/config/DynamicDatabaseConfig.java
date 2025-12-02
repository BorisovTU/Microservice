package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.DynamicDatabaseProperties;

import javax.sql.DataSource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(DynamicDatabaseProperties.class)
public class DynamicDatabaseConfig {

    private final DynamicDatabaseProperties databaseProperties;

    @Bean
    public Map<String, DataSource> dataSources() {
        Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();

        if (databaseProperties.getDatabases() == null) {
            log.warn("No databases configured in properties");
            return dataSourceMap;
        }

        databaseProperties.getDatabases().forEach((dbName, config) -> {
            if (config.isEnabled()) {
                try {
                    DataSource dataSource = createDataSource(config);
                    dataSourceMap.put(dbName, dataSource);
                    log.info("Created DataSource for database: {}", dbName);
                } catch (Exception e) {
                    log.error("Failed to create DataSource for database {}: {}", dbName, e.getMessage());
                }
            } else {
                log.debug("Database {} is disabled", dbName);
            }
        });

        log.info("Created {} DataSource(s)", dataSourceMap.size());
        return dataSourceMap;
    }

    @Bean
    public Map<String, JdbcTemplate> jdbcTemplates(Map<String, DataSource> dataSources) {
        Map<String, JdbcTemplate> jdbcTemplateMap = new ConcurrentHashMap<>();

        dataSources.forEach((dbName, dataSource) -> {
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
            jdbcTemplateMap.put(dbName, jdbcTemplate);
            log.debug("Created JdbcTemplate for database: {}", dbName);
        });

        return jdbcTemplateMap;
    }

    private DataSource createDataSource(DynamicDatabaseProperties.DatabaseConfig config) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(config.getUrl());
        dataSource.setUsername(config.getUsername());
        dataSource.setPassword(config.getPassword());
        dataSource.setMaximumPoolSize(config.getPoolSize());
        dataSource.setMinimumIdle(2);
        dataSource.setConnectionTimeout(30000);
        dataSource.setIdleTimeout(300000);
        dataSource.setMaxLifetime(1200000);
        dataSource.setPoolName("HikariPool-" + config.getType());

        dataSource.addDataSourceProperty("cachePrepStmts", "true");
        dataSource.addDataSourceProperty("prepStmtCacheSize", "250");
        dataSource.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return dataSource;
    }
}