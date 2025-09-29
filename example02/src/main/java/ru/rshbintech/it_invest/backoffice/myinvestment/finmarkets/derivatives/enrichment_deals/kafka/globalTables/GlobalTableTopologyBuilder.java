package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables;

import org.apache.kafka.streams.StreamsBuilder;

public interface GlobalTableTopologyBuilder {
    void build(StreamsBuilder builder);
}
