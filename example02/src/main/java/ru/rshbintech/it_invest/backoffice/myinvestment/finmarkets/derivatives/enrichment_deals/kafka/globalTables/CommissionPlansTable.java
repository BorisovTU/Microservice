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
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.CommissionPlan;

@Component
@RequiredArgsConstructor
public class CommissionPlansTable implements GlobalTableTopologyBuilder{

    private final KafkaConfig kafkaConfig;

    @Getter
    private GlobalKTable<String, CommissionPlan> table;

    public static final JsonSerde<CommissionPlan> VALUE_SERDE = new JsonSerde<>(CommissionPlan.class);
    private static final String STORAGE_NAME = "dv-deals-enricher-commission-plans-store";
    private static final String KEY_PATTERN = "commissionid:%d-planid:%d";

    public String getKey(int commissionID, int planID) {
        return String.format(KEY_PATTERN, commissionID, planID);
    }

    @Override
    public void build(StreamsBuilder builder) {
        String topic = kafkaConfig.getTopic().getCommissionPlans();

        table = builder.globalTable(topic, Consumed.with(Serdes.String(), VALUE_SERDE),
                Materialized.<String, CommissionPlan, KeyValueStore<Bytes, byte[]>>as(STORAGE_NAME)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(VALUE_SERDE)
        );
    }
}
