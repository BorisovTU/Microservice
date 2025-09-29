package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.MarketSchemeMoexLink;

@Component
@RequiredArgsConstructor
public class MarketSchemeMoexLinkTable implements GlobalTableTopologyBuilder {

    private final KafkaConfig kafkaConfig;

    @Getter
    private GlobalKTable<String, MarketSchemeMoexLink> table;

    public static final JsonSerde<MarketSchemeMoexLink> VALUE_SERDE = new JsonSerde<>(MarketSchemeMoexLink.class);
    private static final String STORAGE_NAME = "dv-deals-enricher-market-scheme-store";

    @Override
    public void build(StreamsBuilder builder) {
        String topic = kafkaConfig.getTopic().getMarketSchemeMoexLnk();

        table = builder.globalTable(topic, Consumed.with(Serdes.String(), VALUE_SERDE),
                Materialized.<String, MarketSchemeMoexLink, KeyValueStore<Bytes, byte[]>>as(STORAGE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(VALUE_SERDE));
    }
}
