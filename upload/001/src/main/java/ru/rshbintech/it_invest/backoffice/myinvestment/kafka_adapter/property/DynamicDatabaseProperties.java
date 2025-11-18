package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Свойства для динамической конфигурации множества БД.
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.databases")
public class DynamicDatabaseProperties {

    private Map<String, DatabaseConfig> databases;

    @Data
    public static class DatabaseConfig {
        private String url;
        private String username;
        private String password;
        private String schema;
        private Integer poolSize = 20;
        private boolean enabled = true;
        private String type;
        private PollingConfig polling;
    }

    @Data
    public static class PollingConfig {
        private boolean enabled = true;
        private Long interval = 5000L;
        private Integer priority = 1;
    }
}