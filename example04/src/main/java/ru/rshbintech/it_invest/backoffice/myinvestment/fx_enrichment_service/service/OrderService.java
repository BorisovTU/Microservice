package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.OrderClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.InstrumentDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.PartyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.QtyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawOrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.enums.OrderStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
public class OrderService {

    private static final String MOEX_EXCHANGE_ID = "2";
    private static final DateTimeFormatter FIX_TS = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
    private static final DateTimeFormatter OUT_INSTANT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter DDMMYYYY = DateTimeFormatter.ofPattern("ddMMyyyy");

    private final KafkaStreams kafkaStreams;

    public OrderClientEnrichedDto enrichOrder(RawOrderDto rawOrder) {
        OrderClientEnrichedDto enriched = new OrderClientEnrichedDto();

        String execType = null;
        if (rawOrder.getParties() != null && !rawOrder.getParties().isEmpty()) {
            execType = rawOrder.getParties().get(0).getExecType();
        }

        OrderStatus status = execType != null ? OrderStatus.fromCode(execType) : null;
        enriched.setStatus(status);

        LocalDateTime transactTime = null;
        if (rawOrder.getQtyData() != null && !rawOrder.getQtyData().isEmpty()) {
            String ts = rawOrder.getQtyData().get(0).getTransactTime();
            if (ts != null) {
                transactTime = LocalDateTime.parse(ts, FIX_TS);
                enriched.setLastUpdateTime(transactTime.format(OUT_INSTANT));
            }
        }

        if (status == OrderStatus.NEW && transactTime != null) {
            enriched.setCreatedDate(transactTime.format(OUT_INSTANT));
            if (rawOrder.getOrderId() != null && rawOrder.getOrderId().length() >= 5) {
                enriched.setCode(transactTime.format(DDMMYYYY) + "0" + rawOrder.getOrderId().substring(4));
            }
        }

        if (rawOrder.getOrderId() != null && rawOrder.getOrderId().length() >= 5) {
            enriched.setExternalCode(rawOrder.getOrderId().substring(4));
        }

        if (rawOrder.getParties() != null && !rawOrder.getParties().isEmpty()) {
            PartyDto party = rawOrder.getParties().get(0);
            SubcontractDto subcontract = getSubcontract(party.getAccount());
            if (subcontract != null) {
                enriched.setContractId(subcontract.getId());
                enriched.setClientId(subcontract.getClientId());
            }
        }

        if (rawOrder.getInstrument() != null && !rawOrder.getInstrument().isEmpty()) {
            InstrumentDto instrument = rawOrder.getInstrument().get(0);
            enriched.setFiId(instrument.getSymbol());
            enriched.setPriceType(instrument.getDefinition());

            if ("Валюта".equalsIgnoreCase(instrument.getDefinition()) && instrument.getRateFiId() != null) {
                enriched.setPriceFiId(instrument.getRateFiId());
            }

            if (instrument.getSide() != null) {
                enriched.setDirection(instrument.getSide() == 1 ? "BUY" : "SELL");
            }
        }

        if (rawOrder.getQtyData() != null && !rawOrder.getQtyData().isEmpty()) {
            QtyDto qty = rawOrder.getQtyData().get(0);
            enriched.setAmount(qty.getQty() != null ? qty.getQty() : BigDecimal.ZERO);
            enriched.setPrice(status == OrderStatus.TRADE ? null : (qty.getPrice() != null ? qty.getPrice() : BigDecimal.ZERO));
        }

        enriched.setExchangeId(MOEX_EXCHANGE_ID);

        return enriched;
    }

    private SubcontractDto getSubcontract(String account) {
        if (account == null) return null;

        ReadOnlyKeyValueStore<String, SubcontractDto> store = kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                        "subcontracts-store",
                        QueryableStoreTypes.keyValueStore()
                )
        );
        return store.get(account);
    }
}
