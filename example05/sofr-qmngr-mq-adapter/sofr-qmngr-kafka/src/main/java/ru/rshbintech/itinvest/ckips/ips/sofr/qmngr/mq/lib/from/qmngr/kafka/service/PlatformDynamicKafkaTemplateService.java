package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.LoggingProducerListener;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.integrations.config.Kafka2JdbcProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.configuration.properties.PlatformKafkaProducersProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.configuration.properties.AppKafkaProperties;

/**
 * Сервис динамических продюсеров (KafkaTemplate) для платформенных Kafka.
 */
@Service
@RequiredArgsConstructor
@EnableConfigurationProperties({
    AppKafkaProperties.class,
    PlatformKafkaProducersProperties.class,
    Kafka2JdbcProperties.class
})
public class PlatformDynamicKafkaTemplateService {
  private final Map<String, KafkaTemplate<String, String>> kafkaTemplateMap = new ConcurrentHashMap<>();
  private final AppKafkaProperties appKafkaProperties;
  private final PlatformKafkaProducersProperties platformKafkaProducersProperties;

  private void createKafkaTemplate(String kafkaTemplateId, KafkaProperties kafkaProperties) {
    KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(kafkaProperties.buildProducerProperties())
    );
    kafkaTemplate.setProducerListener(new LoggingProducerListener<>());
    kafkaTemplateMap.put(kafkaTemplateId, kafkaTemplate);
  }

  @PostConstruct
  private void initializerKafkaTemplates() {
    final Map<String, KafkaProperties> platformKafkaProperties = appKafkaProperties.getPlatform();
    if (!CollectionUtils.isEmpty(platformKafkaProperties)) {
      platformKafkaProperties.forEach(this::createKafkaTemplate);
    }
  }

  @PreDestroy
  private void closeKafkaTemplates() {
    kafkaTemplateMap.clear();
  }

  /**
   * Получает KafkaTemplate по названию топика.
   *
   * @param topic топик платформенной кафки
   * @return поставщик сообщений для Kafka (KafkaTemplate)
   */
  public KafkaTemplate<String, String> getKafkaTemplateByTopic(String topic) {
    Optional<String> optionalKafkaTemplateId = platformKafkaProducersProperties.getPlatform().stream()
              .filter(producer -> producer.getTopic().equals(topic))
              .map(PlatformKafkaProducersProperties.PlatformQmngrReadMsgProducerProperties::getKafkaClusterId)
              .findFirst();
    String kafkaTemplateId = optionalKafkaTemplateId.orElse("");
    return kafkaTemplateMap.get(kafkaTemplateId);
  }
}
