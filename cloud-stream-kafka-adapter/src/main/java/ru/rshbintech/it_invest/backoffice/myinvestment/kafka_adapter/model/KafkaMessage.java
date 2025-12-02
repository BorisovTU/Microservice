package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model;

import lombok.Builder;
import lombok.Value;

import java.time.LocalDateTime;

@Value
@Builder
public class KafkaMessage {
    String cluster;
    String topic;
    String messageId;
    LocalDateTime timestamp;
    String headers;
    String payload;
    String sourceDatabase;
}
