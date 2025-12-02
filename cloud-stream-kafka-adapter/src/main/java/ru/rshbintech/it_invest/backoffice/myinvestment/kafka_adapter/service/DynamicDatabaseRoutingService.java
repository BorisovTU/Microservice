package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import com.zaxxer.hikari.HikariDataSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.DatabaseConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.DatabaseRoutingProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.DynamicDatabaseProperties;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DynamicDatabaseRoutingService {

    private final Map<String, JdbcTemplate> jdbcTemplates;
    private final DynamicDatabaseProperties databaseProperties;
    private final DatabaseRoutingProperties routingProperties;

    private final Map<String, DatabaseConfig> databaseConfigs = new ConcurrentHashMap<>();
    private final Map<String, String> topicToDatabaseMapping = new ConcurrentHashMap<>();
    private List<String> pollingOrder = new ArrayList<>();

    @PostConstruct
    public void init() {
        initializeDatabaseConfigs();
        initializeTopicRouting();
        initializePollingOrder();
        log.info("Dynamic database routing initialized with {} databases", databaseConfigs.size());
    }

    private void initializeDatabaseConfigs() {
        if (databaseProperties.getDatabases() == null) return;

        databaseProperties.getDatabases().forEach((dbName, config) -> {
            if (config.isEnabled() && jdbcTemplates.containsKey(dbName)) {
                DatabaseConfig dbConfig = DatabaseConfig.builder()
                        .name(dbName)
                        .type(config.getType())
                        .schema(config.getSchema())
                        .pollingEnabled(config.getPolling().isEnabled())
                        .pollingInterval(config.getPolling().getInterval())
                        .pollingPriority(config.getPolling().getPriority())
                        .jdbcTemplate(jdbcTemplates.get(dbName))
                        .build();

                databaseConfigs.put(dbName, dbConfig);
                log.debug("Initialized database config: {}", dbName);
            }
        });
    }

    private void initializeTopicRouting() {
        if (routingProperties.getIncoming() != null) {
            routingProperties.getIncoming().forEach((topic, dbName) -> {
                if (databaseConfigs.containsKey(dbName)) {
                    topicToDatabaseMapping.put(topic, dbName);
                    log.debug("Mapped topic {} to database {}", topic, dbName);
                } else {
                    log.warn("Database {} not found for topic {}", dbName, topic);
                }
            });
        }
    }

    private void initializePollingOrder() {
        pollingOrder = databaseConfigs.values().stream()
                .filter(DatabaseConfig::isPollingEnabled)
                .sorted((db1, db2) -> {
                    Integer priority1 = routingProperties.getOutgoing().getOrDefault(db1.getName(), db1.getPollingPriority());
                    Integer priority2 = routingProperties.getOutgoing().getOrDefault(db2.getName(), db2.getPollingPriority());
                    return priority1.compareTo(priority2);
                })
                .map(DatabaseConfig::getName)
                .collect(Collectors.toList());

        log.info("Database polling order: {}", pollingOrder);
    }

    public String getTargetDatabaseForTopic(String topic) {
        String database = topicToDatabaseMapping.get(topic);
        if (database == null) {
            database = pollingOrder.isEmpty() ? null : pollingOrder.get(0);
            log.debug("No specific mapping for topic {}, using default database: {}", topic, database);
        }
        return database;
    }

    public List<String> getPollingOrder() {
        return new ArrayList<>(pollingOrder);
    }

    public DatabaseConfig getDatabaseConfig(String databaseName) {
        return databaseConfigs.get(databaseName);
    }

    public Collection<DatabaseConfig> getAllDatabaseConfigs() {
        return databaseConfigs.values();
    }

    public boolean isDatabaseAvailable(String databaseName) {
        DatabaseConfig config = databaseConfigs.get(databaseName);
        if (config == null) return false;

        try {
            config.getJdbcTemplate().execute("SELECT 1");
            return true;
        } catch (Exception e) {
            log.warn("Database {} is unavailable: {}", databaseName, e.getMessage());
            return false;
        }
    }

    public String getSchemaForDatabase(String databaseName) {
        DatabaseConfig config = databaseConfigs.get(databaseName);
        return config != null ? config.getSchema() : null;
    }

    public void addDatabase(String name, DynamicDatabaseProperties.DatabaseConfig config) {
        try {
            HikariDataSource dataSource = createDataSource(config);
            JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

            jdbcTemplates.put(name, jdbcTemplate);

            DatabaseConfig dbConfig = DatabaseConfig.builder()
                    .name(name)
                    .type(config.getType())
                    .schema(config.getSchema())
                    .pollingEnabled(config.getPolling().isEnabled())
                    .pollingInterval(config.getPolling().getInterval())
                    .pollingPriority(config.getPolling().getPriority())
                    .jdbcTemplate(jdbcTemplate)
                    .build();

            databaseConfigs.put(name, dbConfig);
            initializePollingOrder();

            log.info("Dynamically added database: {}", name);

        } catch (Exception e) {
            log.error("Failed to dynamically add database {}: {}", name, e.getMessage());
        }
    }

    private HikariDataSource createDataSource(DynamicDatabaseProperties.DatabaseConfig config) {
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
        return dataSource;
    }
}
