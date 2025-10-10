package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

/**
 * Сервис для логирования настроек подключения к базе данных на тестовых средах.
 */
@Slf4j
@Service
@SuppressWarnings("unused")
@ConditionalOnProperty(prefix = "app.monitoring.db.connection.logging", value = "enabled", havingValue = "true")
public class DataBaseConnectionLoggingService {

  @Value("${APP_ORACLE_HOST}")
  private String appOracleHost;
  @Value("${APP_ORACLE_SCHEMA}")
  private String appOracleSchema;

  public void logDataBaseConnectionProperties() {
    log.info("DataBase connection properties: host = [{}], schema = [{}]", appOracleHost, appOracleSchema);
  }

}
