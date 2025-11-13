package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

/**
 * Модель статуса обработки запроса, используемая для уведомления о результатах обработки.
 *
 * @param status    статус обработки запроса
 * @param detail    детальное описание статуса или ошибки
 * @param requestId уникальный идентификатор запроса
 */
public record RequestStatus(StatusValue status, String detail, String requestId) {

    /**
     * Перечисление возможных статусов обработки запроса.
     */
    public enum StatusValue {
        /**
         * Успешная обработка
         */
        OK,
        /**
         * Ошибка при обработке
         */
        ERROR,
        /**
         * Обработка выполняется
         */
        IN_PROGRESS
    }
}
