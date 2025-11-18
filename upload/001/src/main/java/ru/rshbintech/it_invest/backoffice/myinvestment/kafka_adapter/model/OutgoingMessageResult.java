package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Value;

@Value
public class OutgoingMessageResult {
    KafkaMessage message;
    Integer errorCode;
    String errorDescription;

    public boolean hasMessage() {
        return message != null;
    }

    public boolean isSuccess() {
        return errorCode != null && errorCode == 0;
    }

    public boolean isNoMessages() {
        return errorCode != null && errorCode == 25228;
    }
}
