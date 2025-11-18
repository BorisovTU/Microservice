package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Value;

@Value
public class StoredProcedureResult {
    Integer errorCode;
    String errorDescription;

    public boolean isSuccess() {
        return errorCode != null && errorCode == 0;
    }
}
