package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Сервис для обработки входящих сообщений из Kafka.
 * Получает сообщения от Kafka listeners и делегирует их обработку основному сервису.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaMessageListenerService {

    private final MessageProcessorService messageProcessorService;

    /**
     * Обрабатывает сообщение, полученное из Kafka.
     *
     * @param topic     топик из которого получено сообщение
     * @param key       ключ сообщения (может быть null)
     * @param value     содержимое сообщения
     * @param partition партиция из которой получено сообщение
     * @param offset    оффсет сообщения в партиции
     */
    public void processMessage(String topic, String key, String value, int partition, long offset) {
        try {
            String messageId = key != null ? key : UUID.randomUUID().toString();

            KafkaMessage message = KafkaMessage.builder()
                    .topic(topic)
                    .messageId(messageId)
                    .timestamp(LocalDateTime.now())
                    .headers("{}")
                    .payload(value)
                    .build();

            log.debug("Processing message from topic {} [partition: {}, offset: {}]: {}",
                    topic, partition, offset, messageId);

            messageProcessorService.processIncomingMessage(message);

            log.debug("Successfully processed message from topic {}: {}", topic, messageId);

        } catch (Exception e) {
            log.error("Error processing message from topic {}: {}", topic, e.getMessage(), e);
        }
    }
}
