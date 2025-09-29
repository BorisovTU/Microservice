package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionPlanDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionTypeDto;

@Component
@RequiredArgsConstructor
@Slf4j
public class CommissionsGlobalTables implements StreamStorable {

    private final CommissionsPlansGlobalTableBuilder plansBuilder;
    private final CommissionTypesGlobalTableBuilder typesBuilder;

    private ReadOnlyKeyValueStore<String, CommissionPlanDto> plansStore;
    private ReadOnlyKeyValueStore<String, CommissionTypeDto> typesStore;

    private static final String PLAN_STORE_NAME = "fx-commission-plans-store";
    private static final String TYPE_STORE_NAME = "fx-commission-types-store";

    @Override
    public void saveStore(KafkaStreams streams) {
        plansStore = streams.store(StoreQueryParameters.fromNameAndType(
                PLAN_STORE_NAME,
                QueryableStoreTypes.keyValueStore()
        ));
        typesStore = streams.store(StoreQueryParameters.fromNameAndType(
                TYPE_STORE_NAME,
                QueryableStoreTypes.keyValueStore()
        ));
    }

    public CommissionPlanDto getPlanByContractId(String contractId, String commissionCode) {
        if (plansStore == null) {
            log.warn("Plans store not initialized yet");
            return null;
        }

        CommissionPlanDto plan = plansStore.get(contractId);
        if (plan != null && plan.getCommissionId().equals(commissionCode)) {
            return plan;
        }
        return null;
    }

    public CommissionTypeDto getTypeByCode(String code) {
        if (typesStore == null) {
            log.warn("Types store not initialized yet");
            return null;
        }
        return typesStore.get(code);
    }
}

