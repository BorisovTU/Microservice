package service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.TradeClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.MarketSchemeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.InstrumentDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.PartyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.QtyDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawTradeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.CommissionService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.StateStoreService;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.TradeService;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TradeServiceTest {

    @Mock
    private CommissionService commissionService;
    @Mock
    private StateStoreService stateStoreService;

    @InjectMocks
    private TradeService tradeService;

    private static final String CONTRACT_ID = "123";
    private static final String CLIENT_ID = "456";

    @Test
    void enrichTrade_shouldPopulateFieldsWithSubcontractAndMarketScheme() {
        String account = "ACC1";
        RawTradeDto rawTrade = RawTradeDto.builder()
                .orderId("ORD12345")
                .parties(List.of(PartyDto.builder().account(account).settlDate("2025-09-22").build()))
                .instrument(List.of(InstrumentDto.builder().symbol("USD/RUB").side(1).build()))
                .qtyData(List.of(QtyDto.builder().qty(BigDecimal.valueOf(100)).price(BigDecimal.valueOf(74.5)).transactTime("20250922-15:30:00.000").build()))
                .build();

        SubcontractDto subcontract = SubcontractDto.builder()
                .id(CONTRACT_ID)
                .clientId(CLIENT_ID)
                .build();

        MarketSchemeDto marketScheme = MarketSchemeDto.builder()
                .marketSchemeId(3)
                .build();

        when(stateStoreService.getSubcontract(account)).thenReturn(subcontract);
        when(stateStoreService.getMarketScheme(CONTRACT_ID)).thenReturn(marketScheme);
        when(commissionService.calculateExchangeCommission(any(), any())).thenReturn(BigDecimal.valueOf(10));
        when(commissionService.calculateBrokerCommission(any(), any())).thenReturn(BigDecimal.valueOf(5));

        TradeClientEnrichedDto enriched = tradeService.enrichTrade(rawTrade);

        assertEquals(CONTRACT_ID, enriched.getContractId());
        assertEquals(CLIENT_ID, enriched.getClientId());
        assertEquals(3, enriched.getMarketSchemeId());
        assertEquals("USD/RUB", enriched.getFiId());
        assertEquals("BUY", enriched.getDirection());
        assertEquals(BigDecimal.valueOf(100), enriched.getAmount());
        assertEquals(BigDecimal.valueOf(74.5), enriched.getPrice());
        assertEquals("2", enriched.getExchangeId());
        assertEquals("2", enriched.getCounterPartyId());
        assertEquals("2025-09-22T15:30:00", enriched.getCreatedDate());
        assertNotNull(enriched.getCode());
        assertEquals("2345", enriched.getExternalOrderId());

        assertEquals(2, enriched.getCommissions().size());
    }

    @Test
    void enrichTrade_shouldHandleMissingPartiesAndInstrument() {
        RawTradeDto rawTrade = RawTradeDto.builder().build();

        TradeClientEnrichedDto enriched = tradeService.enrichTrade(rawTrade);

        assertNull(enriched.getContractId());
        assertNull(enriched.getClientId());
        assertNull(enriched.getMarketSchemeId());
        assertNull(enriched.getFiId());
        assertNull(enriched.getDirection());
        assertNull(enriched.getAmount());
        assertNull(enriched.getPrice());
        assertEquals("2", enriched.getExchangeId());
        assertEquals("2", enriched.getCounterPartyId());
        assertNull(enriched.getClearingDate());
        assertNull(enriched.getCode());
        assertNull(enriched.getExternalOrderId());
        assertTrue(enriched.getCommissions().isEmpty());
    }

    @Test
    void enrichTrade_shouldHandleNullTransactTime() {
        RawTradeDto rawTrade = RawTradeDto.builder()
                .orderId("ORD12345")
                .qtyData(List.of(QtyDto.builder().qty(BigDecimal.valueOf(50)).price(BigDecimal.valueOf(100)).build()))
                .build();

        TradeClientEnrichedDto enriched = tradeService.enrichTrade(rawTrade);

        assertNotNull(enriched.getAmount());
        assertNotNull(enriched.getPrice());
        assertNull(enriched.getCreatedDate());
    }
}
