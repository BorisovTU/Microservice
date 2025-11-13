package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception;

/**
 * Исключение, возникающее при ошибках отправки сообщений в Kafka.
 * Используется для ошибок взаимодействия с Kafka брокером.
 */
public class MessageSendException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message описание ошибки
     */
    public MessageSendException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message описание ошибки
     * @param cause   исходное исключение
     */
    public MessageSendException(String message, Throwable cause) {
        super(message, cause);
    }
}
