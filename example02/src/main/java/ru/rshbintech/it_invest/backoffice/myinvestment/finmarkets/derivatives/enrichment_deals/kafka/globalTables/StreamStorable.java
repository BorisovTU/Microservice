package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables;

import org.apache.kafka.streams.KafkaStreams;

public interface StreamStorable {
    void saveStore(KafkaStreams streams);
}
