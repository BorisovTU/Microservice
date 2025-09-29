package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionTypeDto;

@Component
public class CommissionTypesGlobalTableBuilder extends AbstractGlobalTableBuilder<CommissionTypeDto> {

    public CommissionTypesGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getCommissionsTypes();
    }

    @Override
    protected String storeName() {
        return "fx-commission-types-store";
    }

    @Override
    protected Class<CommissionTypeDto> valueClass() {
        return CommissionTypeDto.class;
    }
}
