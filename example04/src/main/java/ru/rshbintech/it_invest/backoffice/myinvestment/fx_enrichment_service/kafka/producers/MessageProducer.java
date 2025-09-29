package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessageProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaConfig kafkaConfig;

    private static final DateTimeFormatter OUT_INSTANT_MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public void sendToDlq(Long timestamp,
                          String key,
                          String value,
                          Iterable<Header> headers,
                          String sourceTopic,
                          long offset,
                          String errorMessage,
                          int attemptCount) {

        Headers dlqHeaders = new RecordHeaders();

        dlqHeaders.add("msgId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        dlqHeaders.add("requestTime", LocalDateTime.now().format(OUT_INSTANT_MILLIS)
                .getBytes(StandardCharsets.UTF_8));
        dlqHeaders.add("traceId", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

        if (headers != null) {
            headers.forEach(dlqHeaders::add);
        }

        dlqHeaders.add("sourceTopic", sourceTopic.getBytes(StandardCharsets.UTF_8));
        dlqHeaders.add("originalOffset", String.valueOf(offset).getBytes(StandardCharsets.UTF_8));
        dlqHeaders.add("deadLetterReason",
                String.format("Сервис: fx-enrichment-service. Ошибка: %s", errorMessage)
                        .getBytes(StandardCharsets.UTF_8));
        dlqHeaders.add("attemptCount", String.valueOf(attemptCount + 1).getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> record = new ProducerRecord<>(
                kafkaConfig.getTopic().getDlq(),
                null,
                timestamp,
                key,
                value,
                dlqHeaders
        );

        kafkaTemplate.send((Message<?>) record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Message was not sent to DLQ: [Topic: {}][Message: {}]",
                                kafkaConfig.getTopic().getDlq(), value, ex);
                    } else {
                        log.info("Sent message to DLQ: [Topic: {}][Message: {}][Attempt: {}]",
                                kafkaConfig.getTopic().getDlq(), value, attemptCount + 1);
                    }
                });
    }
}
