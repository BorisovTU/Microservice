package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Value;

/**
 * Результат чтения исходящего сообщения из базы данных.
 * Содержит сообщение для отправки и метаданные операции чтения.
 */
@Value
public class OutgoingMessageResult {

    /**
     * Сообщение, прочитанное из базы данных для отправки в Kafka.
     * Может быть null, если сообщений нет или произошла ошибка.
     */
    KafkaMessage message;

    /**
     * Код ошибки операции чтения из базы данных.
     */
    Integer errorCode;

    /**
     * Текстовое описание ошибки операции чтения.
     */
    String errorDescription;

    /**
     * Проверяет, содержит ли результат валидное сообщение для отправки.
     *
     * @return true если сообщение не null, иначе false
     */
    public boolean hasMessage() {
        return message != null;
    }

    /**
     * Проверяет, была ли операция чтения успешной.
     *
     * @return true если код ошибки равен 0, иначе false
     */
    public boolean isSuccess() {
        return errorCode != null && errorCode == 0;
    }

    /**
     * Проверяет, указывает ли код ошибки на отсутствие сообщений в очереди.
     *
     * @return true если код ошибки равен 25228 (нет сообщений), иначе false
     */
    public boolean isNoMessages() {
        return errorCode != null && errorCode == 25228;
    }
}
