package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.config.KafkaConfig;

import java.nio.charset.StandardCharsets;

@Service
@RequiredArgsConstructor
@Slf4j
public class DlqService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConfig kafkaConfig;

    public void sendToDlq(String topic, String key, String value, Exception e) {

        Headers headers = new RecordHeaders();
        headers.add("sourceTopic", topic.getBytes(StandardCharsets.UTF_8));
        headers.add("deadLetterReason", String.format("%s. Сервис: %s", e.getMessage(), "fx-clients-trades-sofr").getBytes(StandardCharsets.UTF_8));
        headers.add("attemptCount", "1".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                kafkaConfig.getTopic().getDlq(),
                null,
                key,
                value
        );

        headers.forEach(producerRecord.headers()::add);

        kafkaTemplate.send(producerRecord);

        log.warn("Message sent to DLQ. key={}, reason={}", key, e.getMessage());
    }
}
