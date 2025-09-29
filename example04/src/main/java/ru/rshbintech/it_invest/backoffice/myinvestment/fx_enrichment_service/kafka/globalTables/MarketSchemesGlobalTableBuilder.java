package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.MarketSchemeDto;

@Component
public class MarketSchemesGlobalTableBuilder extends AbstractGlobalTableBuilder<MarketSchemeDto> {

    public MarketSchemesGlobalTableBuilder(KafkaConfig kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected String topic() {
        return kafkaConfig.getTopic().getMarketSchemesMoexLnk();
    }

    @Override
    protected String storeName() {
        return "fx-market-schemes-store";
    }

    @Override
    protected Class<MarketSchemeDto> valueClass() {
        return MarketSchemeDto.class;
    }
}
