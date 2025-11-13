package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

/**
 * Модель сообщения Kafka, содержащая всю необходимую метаинформацию и данные.
 * Используется для передачи сообщений между различными компонентами системы.
 */
@Value
@Builder
public class KafkaMessage {

    String cluster;

    /**
     * Название топика Kafka, в который было отправлено или из которого получено сообщение.
     */
    String topic;

    /**
     * Уникальный идентификатор сообщения. Может быть сгенерирован или получен из ключа Kafka.
     */
    String messageId;

    /**
     * Временная метка создания или получения сообщения.
     */
    LocalDateTime timestamp;

    /**
     * Заголовки сообщения в формате JSON. Содержат дополнительную метаинформацию.
     */
    String headers;

    /**
     * Основное содержимое сообщения (тело сообщения).
     */
    String payload;
}
