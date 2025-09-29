package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.exceptionHandlers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers.MessageProducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private MessageProducer messageProducer;
    private KafkaConfig kafkaConfig;

    @Override
    public void configure(Map<String, ?> configs) {
        this.messageProducer = (MessageProducer) configs.get("dlq-message-producer");
        this.kafkaConfig = (KafkaConfig) configs.get("kafka-config");
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {

        String value = record.value() == null ? null : new String(record.value(), StandardCharsets.UTF_8);
        String key = record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8);

        log.warn("Deserialization error, sending to DLQ; topic: {}, partition: {}, offset: {}, value: {}",
                record.topic(), record.partition(), record.offset(), value, exception);

        messageProducer.sendToDlq(
                record.timestamp(),
                key,
                value,
                record.headers(),
                record.topic(),
                record.offset(),
                exception.getMessage(),
                0
        );

        return DeserializationHandlerResponse.CONTINUE;
    }
}
