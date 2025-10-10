package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.configuration.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные параметры для настройки подключения к различным кластерам платформенной Kafka.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {

  /**
   * Параметры подключений к платформенной kafka в разрезе кластера и информационной системы.
   */
  private Map<String, KafkaProperties> platform;
  /**
   * Глобальные параметры настройки потребителей Kafka.
   */
  @Valid
  private AppKafkaConsumersProperties consumers = new AppKafkaConsumersProperties();

  /**
   * Глобальные настройки потребителей Kafka.
   */
  @Getter
  @Setter
  public static class AppKafkaConsumersProperties {

    /**
     * Интервал переподключения потребителя к Kafka в секундах в случае ошибок подключения.
     */
    @Min(0)
    @NotNull
    private Integer authExceptionRetryInterval = 10;

  }

}
