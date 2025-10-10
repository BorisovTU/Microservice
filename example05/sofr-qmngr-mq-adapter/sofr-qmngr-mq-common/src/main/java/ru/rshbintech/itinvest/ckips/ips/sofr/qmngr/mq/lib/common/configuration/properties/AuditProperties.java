package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные параметры для настройки аудита.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "audit")
public class AuditProperties {

  /**
   * Включен ли аудит.
   */
  private boolean enabled = true;

}
