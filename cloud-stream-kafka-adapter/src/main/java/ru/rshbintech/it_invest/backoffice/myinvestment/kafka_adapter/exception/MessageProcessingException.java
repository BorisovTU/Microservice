package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception;

/**
 * Исключение, возникающее при ошибках обработки сообщений.
 * Используется для ошибок бизнес-логики обработки сообщений.
 */
public class MessageProcessingException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message описание ошибки
     */
    public MessageProcessingException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message описание ошибки
     * @param cause   исходное исключение
     */
    public MessageProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
