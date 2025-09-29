package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.exceptionHandlers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers.MessageProducer;

import java.util.Map;
@Slf4j
public class DlqProcessingExceptionHandler implements ProcessingExceptionHandler {

    private MessageProducer messageProducer;
    private KafkaConfig kafkaConfig;

    @Override
    public void configure(Map<String, ?> configs) {
        this.messageProducer = (MessageProducer) configs.get("dlq-message-producer");
        this.kafkaConfig = (KafkaConfig) configs.get("kafka-config");
    }

    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext context, Record<?, ?> record, Exception exception) {

        String key = record.key() == null ? null : record.key().toString();
        String value = record.value() == null ? null : record.value().toString();

        log.error("Processing error, sending to DLQ; taskId: {}, topic: {}, partition: {}, offset: {}, key: {}, value: {}",
                context.taskId(), context.topic(), context.partition(), context.offset(), key, value, exception);

        messageProducer.sendToDlq(
                record.timestamp(),
                key,
                value,
                record.headers(),
                context.topic(),
                context.offset(),
                exception.getMessage(),
                0
        );

        return ProcessingExceptionHandler.ProcessingHandlerResponse.CONTINUE;
    }
}