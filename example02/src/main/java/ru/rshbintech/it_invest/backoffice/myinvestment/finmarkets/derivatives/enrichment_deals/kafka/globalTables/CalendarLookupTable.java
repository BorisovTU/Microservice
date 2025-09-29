package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.Calendar;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class CalendarLookupTable implements GlobalTableTopologyBuilder, StreamStorable {

    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final KafkaConfig kafkaConfig;

    private ReadOnlyKeyValueStore<String, Calendar> storage;
    private static final JsonSerde<Calendar> VALUE_SERDE = new JsonSerde<>(Calendar.class);
    private static final String STORAGE_NAME = "dv-deals-enricher-calendar-store";

    public void build(StreamsBuilder builder) {
        String topic = kafkaConfig.getTopic().getCalendar();

        builder.globalTable(topic, Consumed.with(Serdes.String(), VALUE_SERDE),
                Materialized.<String, Calendar, KeyValueStore<Bytes, byte[]>>as(STORAGE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(VALUE_SERDE)
        );
    }

    public void saveStore(KafkaStreams streams) {
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Calendar>> storeQueryParameters = StoreQueryParameters.fromNameAndType(
                STORAGE_NAME, QueryableStoreTypes.keyValueStore());

        storage = streams.store(storeQueryParameters);
    }

    public LocalDate findNextWorkDate(LocalDate date) {
        LocalDate checkingDate = date.plusDays(1);
        var isWork = Optional.ofNullable(storage.get(checkingDate.format(DATE_FORMATTER)))
                .map(Calendar::isWork)
                .orElseThrow(() -> new IllegalArgumentException("Calendar for date %s not found".formatted(checkingDate)));
        if (isWork) {
            return checkingDate;
        } else {
            return findNextWorkDate(checkingDate);
        }
    }
}
