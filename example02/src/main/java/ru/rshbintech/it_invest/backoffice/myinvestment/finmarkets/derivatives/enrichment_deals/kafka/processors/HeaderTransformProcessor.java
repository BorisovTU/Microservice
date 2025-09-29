package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.RawDataMessage;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

public class HeaderTransformProcessor extends ContextualFixedKeyProcessor<String, RawDataMessage, RawDataMessage> {
    private static final String HEADER_NAME_TEMPLATE = "enrichment-derivative-deals.%s";

    @Override
    public void process(FixedKeyRecord fixedKeyRecord) {
        Headers headers = fixedKeyRecord.headers();

        UUID msgID = UUID.randomUUID();
        String requestTime = Instant.now().toString();

        headers.add(new RecordHeader(HEADER_NAME_TEMPLATE.formatted("MsgId"), msgID.toString().getBytes(StandardCharsets.UTF_8)))
                .add(new RecordHeader(HEADER_NAME_TEMPLATE.formatted("RequestTime"), requestTime.getBytes(StandardCharsets.UTF_8)));

        context().forward(fixedKeyRecord);
    }
}
