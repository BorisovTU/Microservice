package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;

@Component
public class SubcontractsActiveGlobalTableBuilder extends AbstractGlobalTableBuilder<SubcontractDto> {

    public SubcontractsActiveGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getSubcontractActive();
    }

    @Override
    protected String storeName() {
        return "fx-subcontracts-active-store";
    }

    @Override
    protected Class<SubcontractDto> valueClass() {
        return SubcontractDto.class;
    }
}
