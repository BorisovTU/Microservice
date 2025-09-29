package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.apache.kafka.streams.StreamsBuilder;

public interface GlobalTableTopologyBuilder {
    void build(StreamsBuilder builder);
}
