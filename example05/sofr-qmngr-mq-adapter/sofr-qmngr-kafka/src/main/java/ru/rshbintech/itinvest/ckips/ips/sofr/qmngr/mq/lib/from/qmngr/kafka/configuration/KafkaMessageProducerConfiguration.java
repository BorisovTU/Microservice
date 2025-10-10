package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.configuration;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.lang.NonNull;

/**
 * Конфигурация поставщика сообщений для Kafka.
 */
@Configuration
@SuppressWarnings("unused")
public class KafkaMessageProducerConfiguration {

  /**
   * Метод конфигурирует поставщик сообщений для Kafka (KafkaTemplate).
   *
   * @param kafkaProperties - настройки Kafka
   * @return сконфигурированный поставщик сообщений для Kafka (KafkaTemplate)
   */
  @Bean
  @NonNull
  public KafkaTemplate<String, String> kafkaTemplate(@NonNull KafkaProperties kafkaProperties) {
    KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(
        new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties())
    );
    kafkaTemplate.setProducerListener(new LoggingProducerListener<>());
    return kafkaTemplate;
  }

}
