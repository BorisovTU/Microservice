package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Builder;
import lombok.Data;
import org.springframework.jdbc.core.JdbcTemplate;

@Data
@Builder
public class DatabaseConfig {
    private String name;
    private String type;
    private String schema;
    private boolean pollingEnabled;
    private Long pollingInterval;
    private Integer pollingPriority;
    private JdbcTemplate jdbcTemplate;

    public boolean isPollingEnabled() {
        return pollingEnabled;
    }
}
