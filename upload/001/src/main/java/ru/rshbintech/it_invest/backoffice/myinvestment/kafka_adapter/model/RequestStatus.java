package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

public record RequestStatus(StatusValue status, String detail, String requestId) {

    public enum StatusValue {
        OK,
        ERROR,
        IN_PROGRESS
    }
}
