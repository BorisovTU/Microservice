package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.KafkaHeadersDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.OrderClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.TradeClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawOrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawTradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.producers.MessageProducer;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.OrderService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.TradeService;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class IncomingMessageTopology {

    private final KafkaConfig kafkaConfig;
    private final OrderService orderService;
    private final TradeService tradeService;
    private final MessageProducer messageProducer;
    private final KafkaTemplate<String, OrderClientEnrichedDto> orderKafkaTemplate;
    private final KafkaTemplate<String, TradeClientEnrichedDto> tradeKafkaTemplate;

    private static final JsonSerde<RawOrderDto> RAW_ORDER_SERDE = new JsonSerde<>(RawOrderDto.class);
    private static final JsonSerde<RawTradeDto> RAW_TRADE_SERDE = new JsonSerde<>(RawTradeDto.class);

    private static final DateTimeFormatter OUT_INSTANT_MILLIS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public void buildOrdersStream(StreamsBuilder builder) {
        String rawOrdersTopic = kafkaConfig.getTopic().getRawOrders();
        String enrichedOrdersTopic = kafkaConfig.getTopic().getOrdersClientEnriched();

        KStream<String, RawOrderDto> rawOrdersStream =
                builder.stream(rawOrdersTopic, Consumed.with(Serdes.String(), RAW_ORDER_SERDE));

        rawOrdersStream.foreach((key, rawOrder) -> {
            try {
                OrderClientEnrichedDto enriched = orderService.enrichOrder(rawOrder);
                Headers headers = buildKafkaHeaders();

                ProducerRecord<String, OrderClientEnrichedDto> record =
                        new ProducerRecord<>(enrichedOrdersTopic,
                                null,
                                 System.currentTimeMillis(),
                                 enriched.getClientId(),
                                 enriched,
                                 headers);

                orderKafkaTemplate.send(record);
            } catch (Exception e) {
                log.error("Ошибка при обработке заказа, отправка в DLQ. topic={}, key={}", rawOrdersTopic, key, e);
                messageProducer.sendToDlq(System.currentTimeMillis(),
                        key,
                        rawOrder != null ? rawOrder.toString() : null,
                        null,
                        rawOrdersTopic,
                        -1,
                        e.getMessage(),
                        0);
            }
        });

        log.info("Topology built for raw orders stream: {} -> {}", rawOrdersTopic, enrichedOrdersTopic);
    }

    public void buildTradesStream(StreamsBuilder builder) {
        String rawTradesTopic = kafkaConfig.getTopic().getRawTrades();
        String enrichedTradesTopic = kafkaConfig.getTopic().getTradesClientEnriched();

        KStream<String, RawTradeDto> rawTradesStream =
                builder.stream(rawTradesTopic, Consumed.with(Serdes.String(), RAW_TRADE_SERDE));

        rawTradesStream.foreach((key, rawTrade) -> {
            try {
                TradeClientEnrichedDto enriched = tradeService.enrichTrade(rawTrade);
                Headers headers = buildKafkaHeaders();

                ProducerRecord<String, TradeClientEnrichedDto> record =
                        new ProducerRecord<>(enrichedTradesTopic,
                                null,
                                System.currentTimeMillis(),
                                enriched.getClientId(),
                                enriched,
                                headers);

                tradeKafkaTemplate.send(record);
            } catch (Exception e) {
                log.error("Ошибка при обработке сделки, отправка в DLQ. topic={}, key={}", rawTradesTopic, key, e);
                messageProducer.sendToDlq(System.currentTimeMillis(),
                        key,
                        rawTrade != null ? rawTrade.toString() : null,
                        null,
                        rawTradesTopic,
                        -1,
                        e.getMessage(),
                        0);
            }
        });

        log.info("Topology built for raw trades stream: {} -> {}", rawTradesTopic, enrichedTradesTopic);
    }

    private Headers buildKafkaHeaders() {
        KafkaHeadersDto headersDto = KafkaHeadersDto.builder()
                .msgId(UUID.randomUUID().toString())
                .requestTime(LocalDateTime.now().format(OUT_INSTANT_MILLIS))
                .traceId(UUID.randomUUID().toString())
                .build();

        Headers headers = new RecordHeaders();
        headers.add("msgId", headersDto.getMsgId().getBytes(StandardCharsets.UTF_8));
        headers.add("requestTime", headersDto.getRequestTime().getBytes(StandardCharsets.UTF_8));
        headers.add("traceId", headersDto.getTraceId().getBytes(StandardCharsets.UTF_8));

        return headers;
    }
}
