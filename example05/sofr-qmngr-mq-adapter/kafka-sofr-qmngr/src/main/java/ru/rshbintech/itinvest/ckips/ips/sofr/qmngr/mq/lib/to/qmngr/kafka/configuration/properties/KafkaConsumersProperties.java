package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.model.enums.KafkaType.IPS;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.model.enums.KafkaType;

/**
 * Конфигурационные свойства для настройки потребителей Kafka.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "app.broker-connectors.to.kafka.consumers")
public class KafkaConsumersProperties {

  @Valid
  @NotEmpty
  private List<QmngrLoadMsgConsumerProperties> loadMsg;

  /**
   * Конфигурационные свойства для настройки потребителя сообщений из Kafka с последующим сохранением в SOFR QManager.
   */
  @Getter
  @Setter
  @ToString
  public static class QmngrLoadMsgConsumerProperties {

    /**
     * Топик, из которого будут потребляться сообщения из Kafka.
     */
    @NotBlank
    private String topic;

    /**
     * Нужно ли использовать в качестве уникального идентификатора сообщения ключ сообщения из Kafka.
     */
    private boolean useKeyAsMsgId = true;

    /**
     * Название заголовка, в котором будет находиться уникальный идентификатор сообщения.
     */
    private String msgIdHeaderName;

    /**
     * Тип кластеров Kafka.
     */
    @NotNull
    private KafkaType kafkaType = IPS;

    /**
     * Идентификатор кластера Kafka в настройках приложения.
     */
    private String kafkaClusterId;

  }

}
