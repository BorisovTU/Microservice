package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.configuration.properties.AppKafkaProperties;

/**
 * Конфигурация фабрик слушателей платформенной Kafka.
 */
@Configuration
@SuppressWarnings("unused")
@EnableConfigurationProperties(AppKafkaProperties.class)
public class PlatformKafkaListenerContainerFactoriesConfiguration {

  /**
   * Конфигурация фабрик слушателей платформенной Kafka в разрезе идентификатора кластера Kafka.
   */
  @Bean
  @NonNull
  @Qualifier("platformKafkaListenerContainerFactories")
  public Map<String, ConcurrentKafkaListenerContainerFactory<String, String>> platformKafkaListenerContainerFactories(
      @NonNull AppKafkaProperties appKafkaProperties) {
    final Map<String, KafkaProperties> platformKafkaProperties = appKafkaProperties.getPlatform();
    if (CollectionUtils.isEmpty(platformKafkaProperties)) {
      return Collections.emptyMap();
    }
    return platformKafkaProperties.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> createPlatformKafkaListenerContainerFactory(
                    entry.getValue(),
                    appKafkaProperties.getConsumers().getAuthExceptionRetryInterval()
                )
            )
        );
  }

  @NonNull
  private ConcurrentKafkaListenerContainerFactory<String, String> createPlatformKafkaListenerContainerFactory(
      @NonNull KafkaProperties kafkaProperties,
      @NonNull Integer authExceptionRetryInterval) {
    ConcurrentKafkaListenerContainerFactory<String, String> platformKafkaListenerContainerFactory =
        new ConcurrentKafkaListenerContainerFactory<>();
    platformKafkaListenerContainerFactory.setConsumerFactory(
        new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties())
    );
    /*
    Установка интервала переподключения потребителя к Kafka в секундах в случае ошибок подключения.
    Согласно документации App.Farm сервис подключается к платформенной Kafka не напрямую, а через платформенный Kafka
    адаптер. Адаптер, в свою очередь, может со временем закрывать соединение, что приводит к завершению работы
    потребителя, так как по умолчанию retry интервал в spring-kafka не установлен.
    Для того чтобы этого избежать и устанавливается интервал переподключения. Подробнее в обращении:
    https://gitlab.rshbdev.ru/rshbintech/support-board/-/issues/13269.
     */
    platformKafkaListenerContainerFactory.getContainerProperties().setAuthExceptionRetryInterval(
        Duration.ofSeconds(authExceptionRetryInterval)
    );
    return platformKafkaListenerContainerFactory;
  }

}
