package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;

@RequiredArgsConstructor
public abstract class AbstractGlobalTableBuilder<V> implements GlobalTableTopologyBuilder, StreamStorable {

    protected final KafkaConfig kafkaConfig;
    @Getter
    protected GlobalKTable<String, V> table;

    protected abstract String topic();
    protected abstract String storeName();
    protected abstract Class<V> valueClass();

    @Override
    public void build(StreamsBuilder builder) {
        JsonSerde<V> valueSerde = new JsonSerde<>(valueClass());

        table = builder.globalTable(
                topic(),
                Consumed.with(Serdes.String(), valueSerde),
                Materialized.<String, V, KeyValueStore<Bytes, byte[]>>as(storeName())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(valueSerde)
        );
    }

    @Override
    public void saveStore(KafkaStreams streams) {
    }
}
