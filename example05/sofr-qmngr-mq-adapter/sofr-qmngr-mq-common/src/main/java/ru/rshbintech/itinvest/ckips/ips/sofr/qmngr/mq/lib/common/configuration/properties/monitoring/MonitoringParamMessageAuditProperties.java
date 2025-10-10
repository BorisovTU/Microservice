package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Конфигурационные параметры для глобальной настройки отображения тела сообщения в аудите.
 */
@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "app.monitoring.param.message.audit")
public class MonitoringParamMessageAuditProperties {

  /**
   * Включен ли вывод тела сообщения в аудите.
   */
  private boolean enabled;

  /**
   * Максимальная длина тела сообщения в аудите.
   */
  @NotNull
  @Min(100)
  @Max(700)
  private Integer maxLength = DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;

}
