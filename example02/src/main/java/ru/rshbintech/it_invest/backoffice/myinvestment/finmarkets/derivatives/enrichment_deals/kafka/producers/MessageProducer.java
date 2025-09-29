package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.producers;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config.KafkaConfig;

@Component
@RequiredArgsConstructor
@Slf4j
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConfig kafkaConfig;

    public void SendToDlq(Long timestamp, String key, String value, Iterable<Header> headers) {
        String topic = kafkaConfig.getTopic().getDlq();

        ProducerRecord<String, String> record = new ProducerRecord<>(topic,
                null,
                timestamp,
                key,
                value,
                headers);

        kafkaTemplate.send(record)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Message was not send: [Topic: {}][Message: {}]", topic, record.value(), ex);
                    } else {
                        log.debug("Sent message: [Topic: {}] [Message: {}]", topic, record.value());
                    }
                });
    }
}
