package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import javax.sql.DataSource;

/**
 * Конфигурация базы данных и JDBC компонентов.
 */
@Configuration(proxyBeanMethods = false)
@EnableTransactionManagement
@RequiredArgsConstructor
public class DatabaseConfig {
    @Qualifier("second")
    @Bean(defaultCandidate = false)
    @ConfigurationProperties("app.datasource")
    public DataSourceProperties secondDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Qualifier("second")
    @Bean(defaultCandidate = false)
    @ConfigurationProperties("app.datasource.configuration")
    public HikariDataSource secondDataSource(
            @Qualifier("secondDataSourceProperties") DataSourceProperties secondDataSourceProperties) {
        return secondDataSourceProperties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Qualifier("second")
    @Bean(defaultCandidate = false)
    public JdbcTemplate secondJdbcTemplate(@Qualifier("second") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
