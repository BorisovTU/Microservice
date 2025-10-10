package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.configuration.properties;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные параметры для настройки продюсера сообщений из SOFR QManager в платформенную Kafka.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "app.broker-connectors.from.kafka.producers")
public class PlatformKafkaProducersProperties {

  @Valid
  @NotEmpty
  private List<PlatformQmngrReadMsgProducerProperties> platform;

  /**
   * Конфигурационные параметры для настройки продюсера сообщений из SOFR QManager в платформенную Kafka.
   */
  @Getter
  @Setter
  @ToString
  public static class PlatformQmngrReadMsgProducerProperties {

    /**
     * Топик, в который будут отправляться сообщения из SOFR QManager в платформенную Kafka.
     */
    @NotBlank
    private String topic;

    /**
     * Идентификатор кластера Kafka в настройках приложения.
     */
    private String kafkaClusterId;

  }

}
