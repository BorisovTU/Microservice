package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.kafka.common.model.enums.KafkaType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.configuration.properties.KafkaConsumersProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.listener.QmngrLoadMsgListener;

/**
 * Конфигурация потребителей сообщений из Kafka.
 * Микросервис позиционируется как универсальный. Поэтому при добавлении новых топиков в параметры конфигурации
 * сообщения из них автоматически должны обрабатываться по аналогичному алгоритму. Функционал аннотации KafkaListener
 * позволяет указать список топиков, однако, этот механизм не совсем оптимальный, так как нельзя гарантировать, что
 * чтение будет осуществляться справедливым образом (нет гарантий, что чтение будет осуществляться равномерно), даже
 * при правильно настроенном уровне параллелизма. Поэтому, намного выгоднее создать на каждый топик свой KafkaListener.
 * Но в этом случае при добавлении нового топика придется дорабатывать исходный код, что противоречит поставленной
 * задаче. Поэтому было принято решение сконфигурировать потребители Kafka через интерфейс MessageListener, который
 * позволяет создать их динамически.
 */
@Configuration
@RequiredArgsConstructor
@SuppressWarnings("unused")
@EnableConfigurationProperties(KafkaConsumersProperties.class)
public class KafkaMessageListenersConfiguration {

  private static final String MSG_CONFIG_ERROR = "Invalid platform kafka consumers configuration: %s";

  private final QmngrLoadMsgListener loadMsgListener;
  private final KafkaConsumersProperties kafkaConsumersProperties;
  private final ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory;

  private Map<String, ConcurrentKafkaListenerContainerFactory<String, String>> platformKafkaListenerContainerFactories;

  private final List<ConcurrentMessageListenerContainer<String, String>> messageListenerContainers = new ArrayList<>();

  @Autowired
  @Qualifier("platformKafkaListenerContainerFactories")
  public void setPlatformKafkaListenerContainerFactories(
      Map<String, ConcurrentKafkaListenerContainerFactory<String, String>> platformKafkaListenerContainerFactories) {
    this.platformKafkaListenerContainerFactories = platformKafkaListenerContainerFactories;
  }

  @PostConstruct
  private void init() {
    kafkaConsumersProperties.getLoadMsg().forEach(
        loadMsgConsumerProperties -> messageListenerContainers.add(
            createLoadMsgListenerContainerForTopic(loadMsgConsumerProperties)
        )
    );
  }

  @PreDestroy
  private void stopMessageListenerContainers() {
    messageListenerContainers.forEach(
        messageListenerContainer -> {
          if (messageListenerContainer.isRunning()) {
            messageListenerContainer.stop();
          }
        }
    );
  }

  @NonNull
  private ConcurrentMessageListenerContainer<String, String> createLoadMsgListenerContainerForTopic(
      @NonNull KafkaConsumersProperties.QmngrLoadMsgConsumerProperties loadMsgConsumerProperties) {
    final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
    ConcurrentMessageListenerContainer<String, String> messageListenerContainer =
        getContainerFactory(loadMsgConsumerProperties).createContainer(loadMsgConsumerProperties.getTopic());
    messageListenerContainer.getContainerProperties().setMessageListener(loadMsgListener);
    messageListenerContainer.start();
    return messageListenerContainer;
  }

  @NonNull
  private ConcurrentKafkaListenerContainerFactory<String, String> getContainerFactory(
      @NonNull KafkaConsumersProperties.QmngrLoadMsgConsumerProperties loadMsgConsumerProperties) {
    final KafkaType kafkaType = loadMsgConsumerProperties.getKafkaType();
    return switch (kafkaType) {
      case IPS -> kafkaListenerContainerFactory;
      case PLATFORM -> {
        final String kafkaClusterId = loadMsgConsumerProperties.getKafkaClusterId();
        checkPlatformKafkaConfiguration(kafkaClusterId, loadMsgConsumerProperties);
        yield platformKafkaListenerContainerFactories.get(kafkaClusterId);
      }
    };
  }

  private void checkPlatformKafkaConfiguration(
      @Nullable String kafkaClusterId,
      @NonNull KafkaConsumersProperties.QmngrLoadMsgConsumerProperties loadMsgConsumerProperties) {
    if (StringUtils.isEmpty(kafkaClusterId)) {
      throw new InvalidConfigurationException(
          String.format(
              MSG_CONFIG_ERROR,
              String.format(
                  "empty kafka cluster id in load msg consumer properties = [%s]",
                  loadMsgConsumerProperties
              )
          )
      );
    }
    if (CollectionUtils.isEmpty(platformKafkaListenerContainerFactories)) {
      throw new InvalidConfigurationException(
          String.format(
              MSG_CONFIG_ERROR,
              "empty platform kafka settings in app.kafka.platform property"
          )
      );
    }
    if (!platformKafkaListenerContainerFactories.containsKey(kafkaClusterId)) {
      throw new InvalidConfigurationException(
          String.format(
              MSG_CONFIG_ERROR,
              String.format(
                  "empty platform kafka settings in app.kafka.platform property for kafka cluster with id = [%s]",
                  kafkaClusterId
              )
          )
      );
    }
  }

}
