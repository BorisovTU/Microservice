package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.apache.kafka.streams.KafkaStreams;

public interface StreamStorable {
    void saveStore(KafkaStreams streams);
}
