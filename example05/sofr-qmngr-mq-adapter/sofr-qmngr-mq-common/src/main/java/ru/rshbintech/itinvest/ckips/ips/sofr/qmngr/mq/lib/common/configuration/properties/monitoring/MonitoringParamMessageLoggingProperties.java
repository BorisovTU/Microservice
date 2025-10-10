package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Конфигурационные параметры для глобальной настройки отображения тела сообщения в логах.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.monitoring.param.message.logging")
public class MonitoringParamMessageLoggingProperties {

  /**
   * Включен ли вывод тела сообщения в логах.
   */
  private boolean enabled = true;

  /**
   * Максимальная длина тела сообщения в логах.
   */
  @NotNull
  @Min(100)
  private Integer maxLength = DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;

}
