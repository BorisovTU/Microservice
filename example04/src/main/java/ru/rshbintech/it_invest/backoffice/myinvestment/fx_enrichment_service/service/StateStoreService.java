package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.MarketSchemeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;

@Service
@RequiredArgsConstructor
public class StateStoreService {

    private final KafkaStreams kafkaStreams;

    public SubcontractDto getSubcontract(String account) {
        if (account == null) return null;

        ReadOnlyKeyValueStore<String, SubcontractDto> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "subcontracts-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        return store.get(account);
    }

    public MarketSchemeDto getMarketScheme(String firmId) {
        if (firmId == null) return null;

        ReadOnlyKeyValueStore<String, MarketSchemeDto> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "market-schemes-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        return store.get(firmId);
    }
}

