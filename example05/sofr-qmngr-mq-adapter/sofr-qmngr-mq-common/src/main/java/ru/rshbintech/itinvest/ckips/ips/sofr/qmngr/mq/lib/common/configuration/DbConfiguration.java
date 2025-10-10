package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(
    DataSourceProperties.class
)
public class DbConfiguration {
  private final DataSourceProperties dataSourceProperties;

  public DbConfiguration(DataSourceProperties dataSourceProperties) {
    this.dataSourceProperties = dataSourceProperties;
  }

  @Bean
  public DataSource dataSource() {
    return DataSourceBuilder.create()
        .driverClassName(dataSourceProperties.getDriverClassName())
        .url(dataSourceProperties.getUrl())
        .username(dataSourceProperties.getUsername())
        .password(dataSourceProperties.getPassword())
        .build();
  }
}
