package ru.rshbintech.it_invest.backoffice.myinvestment.enrichment_deals.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.Contract;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.FinancialInstrument;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.FixDerivativeDataRaw;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.RequestEnriched;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.RequestService;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@ExtendWith(MockitoExtension.class)
public class RequestServiceTest {

    @InjectMocks
    private RequestService requestService;

    private FinancialInstrument createFinancialInstrumentFuture() {
        FinancialInstrument financialInstrument = new FinancialInstrument(
                1234L,
                "marketPlaceCode",
                "Фьючерс",
                BigDecimal.valueOf(8.0),
                "faceValueKind",
                0L,
                0,
                BigDecimal.valueOf(0.0),
                0L,
                BigDecimal.valueOf(10.0),
                "drawingDate",
                BigDecimal.valueOf(20.0)
        );

        return financialInstrument;
    }

    private FinancialInstrument createFinancialInstrumentOption() {
        FinancialInstrument financialInstrument = new FinancialInstrument(
                2345L,
                "marketPlaceCode",
                "Опцион",
                BigDecimal.valueOf(8.0),
                "faceValueKind",
                333L,
                1,
                BigDecimal.valueOf(3.7),
                0L,
                BigDecimal.valueOf(10.0),
                "drawingDate",
                BigDecimal.valueOf(20.0)
        );

        return financialInstrument;
    }

    @Test
    void createBaseRequest_newRequest_success() {
        FixDerivativeDataRaw fixDerivativeDataRaw = new FixDerivativeDataRaw();
        fixDerivativeDataRaw.setTransactTime("20251101-13:15:16.223");
        fixDerivativeDataRaw.setSide("1");
        fixDerivativeDataRaw.setOrderId(7654321L);
        fixDerivativeDataRaw.setOrderQty(23L);
        fixDerivativeDataRaw.setPrice(BigDecimal.valueOf(24.54));
        fixDerivativeDataRaw.setExecType("0");
        fixDerivativeDataRaw.setFlags(0x4000000L);
        fixDerivativeDataRaw.setComplianceId("D");

        RequestEnriched requestEnriched = requestService.createBaseRequest(fixDerivativeDataRaw);

        assertEquals("321", requestEnriched.getExternalCode());
        assertEquals("BUY", requestEnriched.getDirection());
        assertEquals(23, requestEnriched.getAmount());
        assertEquals(BigDecimal.valueOf(24.54), requestEnriched.getPrice());
        assertEquals("NEW", requestEnriched.getStatus());
        assertEquals("2025-11-01T13:15:16.223", requestEnriched.getLastUpdateTime());
        assertEquals(true, requestEnriched.getIsAddress());
        assertEquals(true, requestEnriched.getIsMarginCall());
        assertEquals("011120250321", requestEnriched.getCode());
        assertEquals("2025-11-01T13:15:16.223", requestEnriched.getCreatedDate());
        assertEquals("2", requestEnriched.getExchangeId());
    }

    @Test
    void setInstrumentSpecificFields_future() {
        RequestEnriched requestEnriched = new RequestEnriched();
        FinancialInstrument financialInstrument = createFinancialInstrumentFuture();

        requestService.setInstrumentSpecificFields(requestEnriched, financialInstrument);

        assertEquals("1234", requestEnriched.getFiId());
        assertEquals("Пункты", requestEnriched.getPriceType());
        assertNull(requestEnriched.getPriceFiId());
    }

    @Test
    void setInstrumentSpecificFields_option() {
        RequestEnriched requestEnriched = new RequestEnriched();
        FinancialInstrument financialInstrument = createFinancialInstrumentOption();

        requestService.setInstrumentSpecificFields(requestEnriched, financialInstrument);

        assertEquals("2345", requestEnriched.getFiId());
        assertEquals("Валюта", requestEnriched.getPriceType());
        assertEquals("333", requestEnriched.getPriceFiId());
    }

    @Test
    void setContractSpecificFields() {
        RequestEnriched requestEnriched = new RequestEnriched();

        Contract contract = new Contract(444L, "mpcode", 555L, "firmId", 1);

        requestService.setContractSpecificFields(requestEnriched, contract);

        assertEquals("444", requestEnriched.getContractId());
        assertEquals("555", requestEnriched.getClientId());
    }
}
