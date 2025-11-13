package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Value;

/**
 * Результат вызова хранимой процедуры в базе данных.
 * Содержит код ошибки и описание, возвращаемые хранимой процедурой.
 */
@Value
public class StoredProcedureResult {

    /**
     * Код ошибки, возвращаемый хранимой процедурой.
     * 0 означает успешное выполнение.
     */
    Integer errorCode;

    /**
     * Текстовое описание ошибки или результата выполнения.
     */
    String errorDescription;

    /**
     * Проверяет, было ли выполнение хранимой процедуры успешным.
     *
     * @return true если код ошибки равен 0, иначе false
     */
    public boolean isSuccess() {
        return errorCode != null && errorCode == 0;
    }
}
