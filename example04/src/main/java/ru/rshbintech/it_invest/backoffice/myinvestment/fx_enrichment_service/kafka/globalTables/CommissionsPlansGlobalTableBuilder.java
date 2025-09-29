package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionPlanDto;

@Component
public class CommissionsPlansGlobalTableBuilder extends AbstractGlobalTableBuilder<CommissionPlanDto> {

    public CommissionsPlansGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getCommissionPlans();
    }

    @Override
    protected String storeName() {
        return "fx-commission-plans-store";
    }

    @Override
    protected Class<CommissionPlanDto> valueClass() {
        return CommissionPlanDto.class;
    }
}
