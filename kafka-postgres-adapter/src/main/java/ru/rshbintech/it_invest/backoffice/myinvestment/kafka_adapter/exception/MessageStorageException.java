package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception;

/**
 * Исключение, возникающее при ошибках работы с хранилищем данных.
 * Используется для ошибок взаимодействия с базой данных.
 */
public class MessageStorageException extends RuntimeException {

    /**
     * Создает новое исключение с указанным сообщением.
     *
     * @param message описание ошибки
     */
    public MessageStorageException(String message) {
        super(message);
    }

    /**
     * Создает новое исключение с указанным сообщением и причиной.
     *
     * @param message описание ошибки
     * @param cause   исходное исключение
     */
    public MessageStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
