package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractMoexLnkDto;

@Component
public class SubcontractsMoexLnkGlobalTableBuilder extends AbstractGlobalTableBuilder<SubcontractMoexLnkDto> {

    public SubcontractsMoexLnkGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getSubcontractActiveMoexLnk();
    }

    @Override
    protected String storeName() {
        return "fx-subcontracts-moex-lnk-store";
    }

    @Override
    protected Class<SubcontractMoexLnkDto> valueClass() {
        return SubcontractMoexLnkDto.class;
    }
}
