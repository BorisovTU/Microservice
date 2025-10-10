package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные параметры для настройки логирования параметров подключения к базе данных.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "app.monitoring.db.connection.logging")
public class MonitoringDataBaseConnectionProperties {

  /**
   * Включено ли логирование параметров подключения к базе данных.
   */
  private boolean enabled = false;

}
