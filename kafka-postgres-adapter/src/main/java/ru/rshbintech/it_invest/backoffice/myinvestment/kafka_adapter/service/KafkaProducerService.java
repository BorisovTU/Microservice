package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception.MessageSendException;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.RequestStatus;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.KafkaProperties;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Сервис для отправки сообщений в Kafka.
 * Поддерживает отправку в разные топики с разными настройками кластеров.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;
    private final KafkaConfig.KafkaTemplateFactory kafkaTemplateFactory;
    private final Map<String, KafkaTemplate<String, String>> kafkaTemplates = new ConcurrentHashMap<>();

    /**
     * Отправляет сообщение в Kafka топик.
     *
     * @param message сообщение для отправки
     * @throws MessageSendException если произошла ошибка при отправке сообщения
     */
    public void sendMessage(KafkaMessage message) {
        try {
            KafkaTemplate<String, String> kafkaTemplate = getKafkaTemplateForTopic(message.getTopic());

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(message.getTopic(), message.getMessageId(), message.getPayload());

            future.whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to send message to topic {}: {}", message.getTopic(), exception.getMessage(), exception);
                    throw new MessageSendException("Failed to send message to Kafka", exception);
                } else {
                    log.debug("Successfully sent message to topic {}: {}", message.getTopic(), message.getMessageId());
                }
            });

        } catch (Exception e) {
            log.error("Error sending message to Kafka topic {}: {}", message.getTopic(), e.getMessage(), e);
            throw new MessageSendException("Failed to send message", e);
        }
    }

    /**
     * Отправляет статус запроса в указанный топик Kafka.
     *
     * @param requestStatus статус запроса для отправки
     * @param topic         топик для отправки статуса
     * @throws MessageSendException если произошла ошибка при отправке статуса
     */
    public void sendRequestStatus(RequestStatus requestStatus, String topic) {
        try {
            KafkaTemplate<String, String> kafkaTemplate = getKafkaTemplateForTopic(topic);
            String payload = objectMapper.writeValueAsString(requestStatus);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(topic, requestStatus.requestId(), payload);

            future.whenComplete((result, exception) -> {
                if (exception != null) {
                    log.error("Failed to send request status to topic {}: {}", topic, exception.getMessage(), exception);
                    throw new MessageSendException("Failed to send request status to Kafka", exception);
                } else {
                    log.debug("Successfully sent request status to topic {}: {}", topic, requestStatus.requestId());
                }
            });

        } catch (Exception e) {
            log.error("Error sending request status to Kafka topic {}: {}", topic, e.getMessage(), e);
            throw new MessageSendException("Failed to send request status", e);
        }
    }

    /**
     * Проверяет, поддерживается ли отправка сообщений в указанный топик.
     *
     * @param topic топик для проверки
     * @return true если топик поддерживается, иначе false
     */
    public boolean supportsTopic(String topic) {
        return kafkaProperties.getProducers().stream()
                .anyMatch(config -> config.getTopic().equals(topic));
    }

    /**
     * Получает или создает KafkaTemplate для указанного топика.
     *
     * @param topic топик для которого нужен KafkaTemplate
     * @return настроенный KafkaTemplate для топика
     */
    private KafkaTemplate<String, String> getKafkaTemplateForTopic(String topic) {
        return kafkaTemplates.computeIfAbsent(topic, this::createKafkaTemplateForTopic);
    }

    /**
     * Создает новый KafkaTemplate для указанного топика.
     *
     * @param topic топик для которого создается KafkaTemplate
     * @return новый настроенный KafkaTemplate
     */
    private KafkaTemplate<String, String> createKafkaTemplateForTopic(String topic) {
        String bootstrapServers = kafkaProperties.getProducers().stream()
                .filter(config -> config.getTopic().equals(topic))
                .findFirst()
                .orElseThrow(() -> new MessageSendException("No producer configuration for topic: " + topic))
                .getBootstrapServers();

        return kafkaTemplateFactory.createKafkaTemplate(bootstrapServers);
    }
}
