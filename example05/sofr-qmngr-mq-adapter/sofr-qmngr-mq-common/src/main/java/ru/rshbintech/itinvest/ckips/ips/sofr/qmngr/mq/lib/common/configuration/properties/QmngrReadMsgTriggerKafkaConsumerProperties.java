package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Конфигурационные параметры настройки потребителя сообщений планировщика задач (task-scheduler-service) из
 * Kafka для запуска процесса загрузки сообщений из QManager и отправки их в MQ.
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "app.broker-connectors.from.kafka.consumers.read-msg-trigger")
public class QmngrReadMsgTriggerKafkaConsumerProperties {

  /**
   * Топик, из которого будут потребляться сообщения планировщика задач (task-scheduler-service) из Kafka, для запуска
   * процесса загрузки сообщения из QManager.
   */
  @NotBlank
  private String topic;
  /**
   * Лимит сообщений, которые могут быть прочитаны за один запуск периодического задания по триггеру.
   */
  @NotNull
  @Min(1)
  private int msgReadLimit = 100;

}
