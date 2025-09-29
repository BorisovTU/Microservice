package service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.OrderClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.InstrumentDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.PartyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.QtyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawOrderDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.enums.OrderStatus;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.OrderService;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OrderServiceTest {

    private static final String ACCOUNT = "ACC123";
    private static final String CONTRACT_ID = "CONTRACT1";
    private static final String CLIENT_ID = "CLIENT1";

    @Mock
    private KafkaStreams kafkaStreams;

    @Mock
    private ReadOnlyKeyValueStore<String, SubcontractDto> store;

    @InjectMocks
    private OrderService orderService;

    @Test
    void enrichOrder_shouldPopulateBasicFieldsWithHeaders() {

        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);
        when(store.get(ACCOUNT)).thenReturn(new SubcontractDto(CONTRACT_ID, CLIENT_ID, null));

        RawOrderDto rawOrder = RawOrderDto.builder()
                .orderId("ORD12345")
                .parties(List.of(PartyDto.builder()
                        .account(ACCOUNT)
                        .execType("0")
                        .build()))
                .instrument(List.of(InstrumentDto.builder()
                        .symbol("USD/RUB")
                        .definition("Валюта")
                        .side(1)
                        .rateFiId("RUB")
                        .build()))
                .qtyData(List.of(QtyDto.builder()
                        .qty(BigDecimal.valueOf(100))
                        .price(BigDecimal.valueOf(74.5))
                        .transactTime("20250922-15:30:00.000")
                        .build()))
                .build();

        SubcontractDto subcontract = SubcontractDto.builder()
                .id(CONTRACT_ID)
                .clientId(CLIENT_ID)
                .build();
        when(store.get(ACCOUNT)).thenReturn(subcontract);

        OrderClientEnrichedDto enriched = orderService.enrichOrder(rawOrder);

        assertEquals(OrderStatus.NEW, enriched.getStatus());
        assertEquals("USD/RUB", enriched.getFiId());
        assertEquals("Валюта", enriched.getPriceType());
        assertEquals("RUB", enriched.getPriceFiId());
        assertEquals("BUY", enriched.getDirection());
        assertEquals(BigDecimal.valueOf(100), enriched.getAmount());
        assertEquals(BigDecimal.valueOf(74.5), enriched.getPrice());
        assertEquals(CONTRACT_ID, enriched.getContractId());
        assertEquals(CLIENT_ID, enriched.getClientId());
        assertEquals("2", enriched.getExchangeId());
        assertNotNull(enriched.getCreatedDate());
        assertNotNull(enriched.getLastUpdateTime());
        assertNotNull(enriched.getCode());
        assertEquals("2345", enriched.getExternalCode());
    }

    @Test
    void enrichOrder_shouldHandleMissingPartiesAndInstrument() {
        RawOrderDto rawOrder = RawOrderDto.builder().build();

        OrderClientEnrichedDto enriched = orderService.enrichOrder(rawOrder);

        assertNull(enriched.getStatus());
        assertNull(enriched.getContractId());
        assertNull(enriched.getClientId());
        assertNull(enriched.getFiId());
        assertNull(enriched.getPriceType());
        assertNull(enriched.getPriceFiId());
        assertNull(enriched.getDirection());
        assertNull(enriched.getAmount());
        assertNull(enriched.getPrice());
        assertEquals("2", enriched.getExchangeId());

    }

    @Test
    void enrichOrder_shouldHandleInstrumentNotCurrency() {
        RawOrderDto rawOrder = RawOrderDto.builder()
                .instrument(List.of(InstrumentDto.builder()
                        .symbol("TEST")
                        .definition("Акция")
                        .side(2)
                        .build()))
                .qtyData(List.of(QtyDto.builder()
                        .qty(BigDecimal.valueOf(50))
                        .price(BigDecimal.valueOf(200))
                        .transactTime("20250922-12:00:00.000")
                        .build()))
                .build();

        OrderClientEnrichedDto enriched = orderService.enrichOrder(rawOrder);

        assertEquals("TEST", enriched.getFiId());
        assertEquals("Акция", enriched.getPriceType());
        assertNull(enriched.getPriceFiId());
        assertEquals("SELL", enriched.getDirection());
    }

    @Test
    void enrichOrder_shouldReturnNullSubcontractIfAccountNotFound() {

        when(kafkaStreams.store(any(StoreQueryParameters.class))).thenReturn(store);

        RawOrderDto rawOrder = RawOrderDto.builder()
                .parties(List.of(PartyDto.builder().account("UNKNOWN").build()))
                .build();

        when(store.get("UNKNOWN")).thenReturn(null);

        OrderClientEnrichedDto enriched = orderService.enrichOrder(rawOrder);

        assertNull(enriched.getContractId());
        assertNull(enriched.getClientId());

    }
}
