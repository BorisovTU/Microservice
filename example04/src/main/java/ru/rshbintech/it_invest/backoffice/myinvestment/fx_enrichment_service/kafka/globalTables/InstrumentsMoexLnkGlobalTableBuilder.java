package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.InstrumentMoexLnkDto;

@Component
public class InstrumentsMoexLnkGlobalTableBuilder extends AbstractGlobalTableBuilder<InstrumentMoexLnkDto> {

    public InstrumentsMoexLnkGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getInstrumentsMoexLnk();
    }

    @Override
    protected String storeName() {
        return "fx-instruments-moex-lnk-store";
    }

    @Override
    protected Class<InstrumentMoexLnkDto> valueClass() {
        return InstrumentMoexLnkDto.class;
    }
}
