package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.exceptionHandlers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.producers.MessageProducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class DlqDeserializationExceptionHandler implements DeserializationExceptionHandler {
    private MessageProducer messageProducer;

    @Override
    public void configure(Map<String, ?> map) {
        this.messageProducer = (MessageProducer) map.get("dlq-message-producer");
    }

    @Override
    public DeserializationHandlerResponse handle(ErrorHandlerContext context,
                                                 ConsumerRecord<byte[], byte[]> record,
                                                 Exception exception) {

        String value = new String(record.value(), StandardCharsets.UTF_8);

        log.warn("Exception caught during Deserialization, sending to the dead queue topic; " +
                        "taskId: {}, topic: {}, partition: {}, offset: {}, value: {}",
                context.taskId(), record.topic(), record.partition(), record.offset(), value,
                exception);

        record.headers().add(new RecordHeader("source_topic", record.topic().getBytes(StandardCharsets.UTF_8)));

        String key = record.key() == null ? null : new String(record.key());

        messageProducer.SendToDlq(record.timestamp(), key, new String(record.value()), record.headers());

        return DeserializationHandlerResponse.CONTINUE;
    }
}
