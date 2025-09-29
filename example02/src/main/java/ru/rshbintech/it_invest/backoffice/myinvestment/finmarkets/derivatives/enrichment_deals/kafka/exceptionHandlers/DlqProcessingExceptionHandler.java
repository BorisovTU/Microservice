package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.exceptionHandlers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.errors.ProcessingExceptionHandler;
import org.apache.kafka.streams.processor.api.Record;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.producers.MessageProducer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class DlqProcessingExceptionHandler implements ProcessingExceptionHandler {

    private MessageProducer messageProducer;

    @Override
    public void configure(Map<String, ?> map) {
        this.messageProducer = (MessageProducer) map.get("dlq-message-producer");
    }

    @Override
    public ProcessingHandlerResponse handle(ErrorHandlerContext errorHandlerContext, Record<?, ?> record, Exception e) {

        log.error("Exception caught during processing message, sending to the dead queue topic; " +
                        "taskId: {}, topic: {}, partition: {}, offset: {}, value: {}",
                errorHandlerContext.taskId(), errorHandlerContext.topic(), errorHandlerContext.partition(), errorHandlerContext.offset(), record.value(),
                e);

        record.headers().add(new RecordHeader("source_topic", errorHandlerContext.topic().getBytes(StandardCharsets.UTF_8)));

        messageProducer.SendToDlq(record.timestamp(), String.valueOf(record.key()), String.valueOf(record.value()), record.headers());

        return ProcessingHandlerResponse.CONTINUE;
    }
}
