package ru.rshbintech.it_invest.backoffice.myinvestment.enrichment_deals.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.IncorrectNumberOfLegsException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.PriceNotDeterminedException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.CommissionService;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.DealService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DealServiceTest {

    @Mock
    private CommissionService commissionService;

    @InjectMocks
    private DealService dealService;

    private FinancialInstrument createFinancialInstrumentFuture(long tickFiId) {
        FinancialInstrument financialInstrument = new FinancialInstrument(
                1234L,
                "marketPlaceCode",
                "Фьючерс",
                BigDecimal.valueOf(8.0),
                "faceValueKind",
                0L,
                0,
                BigDecimal.valueOf(0.0),
                tickFiId,
                BigDecimal.valueOf(10.0),
                "drawingDate",
                BigDecimal.valueOf(20.0)
        );

        return financialInstrument;
    }

    private FinancialInstrument createFinancialInstrumentOption(int priceMode) {
        FinancialInstrument financialInstrument = new FinancialInstrument(
                2345L,
                "marketPlaceCode",
                "Опцион",
                BigDecimal.valueOf(8.0),
                "faceValueKind",
                0L,
                priceMode,
                BigDecimal.valueOf(3.7),
                0L,
                BigDecimal.valueOf(10.0),
                "drawingDate",
                BigDecimal.valueOf(20.0)
        );

        return financialInstrument;
    }

    private Commission createCommission() {
        return new Commission("comId", BigDecimal.valueOf(1.0), 2.0);
    }

    private CommissionType createCommissionType() {
        return new CommissionType(1, "code", BigDecimal.valueOf(2000.0), BigDecimal.valueOf(15000.0));
    }

    private CommissionPlan createCommissionPlan() {
        CommissionPlan commissionPlan = new CommissionPlan("COMMISSION_ID",
                1,
                0,
                0.0,
                0.0,
                true,
                false,
                "QTY",
                0.0);

        return commissionPlan;
    }

    @Test
    void createBaseDeal_success() {
        FixDerivativeDataRaw fixDerivativeDataRaw = new FixDerivativeDataRaw();
        fixDerivativeDataRaw.setTransactTime("20251101-13:15:16.223");
        fixDerivativeDataRaw.setSecondaryExecId(1234567L);
        fixDerivativeDataRaw.setOrderId(7654321L);

        DealEnriched dealEnriched = dealService.createBaseDeal(fixDerivativeDataRaw);

        assertEquals("TRADE", dealEnriched.getDealKind());
        assertEquals(5, dealEnriched.getPoint());
        assertEquals("2", dealEnriched.getCounterPartyId());
        assertEquals("2", dealEnriched.getExchangeId());
        assertEquals(true, dealEnriched.getIsPrognose());
        assertEquals("2025-11-01T13:15:16.223", dealEnriched.getCreatedDate());
        assertEquals("1234567", dealEnriched.getExternalCode());
        assertEquals("2025-11-01", dealEnriched.getClearingDate());
        assertEquals("321", dealEnriched.getRequestExternalCode());
        assertNull(dealEnriched.getIsMarginCall());
    }

    @Test
    void setLegSpecificFields_success() {
        DealEnriched dealEnriched = new DealEnriched();
        dealEnriched.setCreatedDate("2025-11-01T13:15:16.223");

        dealService.setLegSpecificFields(dealEnriched, "1", 4.2, 678987L);

        assertEquals("BUY", dealEnriched.getDirection());
        assertEquals(4.2, dealEnriched.getAmount());
        assertEquals("B/011120250678987", dealEnriched.getCode());
    }

    @Test
    void setInstrumentSpecificFields_Future_success() {
        DealEnriched dealEnriched = new DealEnriched();
        FinancialInstrument financialInstrument = createFinancialInstrumentFuture(0);

        dealService.setInstrumentSpecificFields(dealEnriched, financialInstrument, BigDecimal.valueOf(6.8));

        assertEquals("1234", dealEnriched.getFiId());
        assertEquals(BigDecimal.valueOf(6.8), dealEnriched.getPrice());
    }

    @Test
    void setInstrumentSpecificFields_Option_success() {
        DealEnriched dealEnriched = new DealEnriched();
        FinancialInstrument financialInstrument = createFinancialInstrumentOption(1);

        dealService.setInstrumentSpecificFields(dealEnriched, financialInstrument, BigDecimal.valueOf(6.8));

        assertEquals("2345", dealEnriched.getFiId());
        assertEquals(BigDecimal.valueOf(6.8), dealEnriched.getBonus());
        assertEquals(BigDecimal.valueOf(3.7), dealEnriched.getPrice());
    }

    @Test()
    void setInstrumentSpecificFields_Option_PriceNotDeterminedException() {
        DealEnriched dealEnriched = new DealEnriched();
        FinancialInstrument financialInstrument = createFinancialInstrumentOption(0);

        assertThrows(PriceNotDeterminedException.class, () -> dealService.setInstrumentSpecificFields(dealEnriched, financialInstrument, BigDecimal.valueOf(6.8)));
    }

    @Test
    void setPrices_RUB_success() {
        DealEnriched dealEnriched = new DealEnriched();
        dealEnriched.setPrice(BigDecimal.valueOf(1.0));
        dealEnriched.setAmount(5.0);

        FinancialInstrument financialInstrument = createFinancialInstrumentFuture(0L);

        dealService.setPrices(dealEnriched, financialInstrument);

        assertEquals(BigDecimal.valueOf(2.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPriceRUB().setScale(2, RoundingMode.HALF_UP));
        assertEquals(BigDecimal.valueOf(2.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPrice2().setScale(2, RoundingMode.HALF_UP));
        assertEquals(BigDecimal.valueOf(10.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPositionCost().setScale(2, RoundingMode.HALF_UP));
    }

    @Test
    void setPrices_notRUB_success() {
        DealEnriched dealEnriched = new DealEnriched();
        dealEnriched.setPrice(BigDecimal.valueOf(1.0));
        dealEnriched.setAmount(5.0);

        FinancialInstrument financialInstrument = createFinancialInstrumentFuture(1L);

        dealService.setPrices(dealEnriched, financialInstrument);

        assertEquals(BigDecimal.valueOf(2.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPriceRUB().setScale(2, RoundingMode.HALF_UP));
        assertEquals(BigDecimal.valueOf(8.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPrice2().setScale(2, RoundingMode.HALF_UP));
        assertEquals(BigDecimal.valueOf(40.0).setScale(2, RoundingMode.HALF_UP), dealEnriched.getPositionCost().setScale(2, RoundingMode.HALF_UP));
    }

    @Test
    void setContractSpecificFields_success() {
        DealEnriched dealEnriched = new DealEnriched();

        Contract contract = new Contract(444L, "mpcode", 555L, "firmId", 1);

        dealService.setContractSpecificFields(dealEnriched, contract);

        assertEquals("444", dealEnriched.getContractId());
        assertEquals("555", dealEnriched.getClientId());
    }

    @Test
    void setMarketScheme_success() {
        DealEnriched dealEnriched = new DealEnriched();
        MarketSchemeMoexLink marketSchemeMoexLink = new MarketSchemeMoexLink("id", 10);

        dealService.setMarketScheme(dealEnriched, marketSchemeMoexLink);

        assertEquals(10, dealEnriched.getMarketScheme());
    }

    @Test
    void putExchangeCommission_success() {
        DealEnriched dealEnriched = new DealEnriched();

        when(commissionService.getExchangeCommission(anyInt(), any())).thenReturn(createCommission());

        dealService.putExchangeCommission(dealEnriched, 1, BigDecimal.valueOf(2.0));

        assertEquals(1, dealEnriched.getCommissions().size());
    }

    @Test
    void putExchangeCommission_fail() {
        DealEnriched dealEnriched = new DealEnriched();

        when(commissionService.getExchangeCommission(anyInt(), any())).thenReturn(null);

        dealService.putExchangeCommission(dealEnriched, 1, BigDecimal.valueOf(0));

        assertNull(dealEnriched.getCommissions());
    }

    @Test
    void putClientCommission_success() {
        DealEnriched dealEnriched = new DealEnriched();
        dealEnriched.setAmount(2.0);

        CommissionType commissionType = createCommissionType();
        CommissionPlan commissionPlan = createCommissionPlan();

        when(commissionService.calculateBrokerageCommission(any(), any(), any())).thenReturn(createCommission());

        dealService.putClientCommission(dealEnriched, commissionType, commissionPlan);

        assertEquals(1, dealEnriched.getCommissions().size());
    }

    @Test
    void putClientCommission_fail() {
        DealEnriched dealEnriched = new DealEnriched();
        dealEnriched.setAmount(2.00);

        CommissionType commissionType = createCommissionType();
        CommissionPlan commissionPlan = createCommissionPlan();

        when(commissionService.calculateBrokerageCommission(any(), any(), any())).thenReturn(null);

        dealService.putClientCommission(dealEnriched, commissionType, commissionPlan);

        assertNull(dealEnriched.getCommissions());
    }

    @Test
    void validateTwoLeggedDeal_success() {
        FixDerivativeDataRaw fixDerivativeDataRaw = new FixDerivativeDataRaw();
        FixDerivativeDataRaw.Leg leg1 = new FixDerivativeDataRaw.Leg();
        FixDerivativeDataRaw.Leg leg2 = new FixDerivativeDataRaw.Leg();
        fixDerivativeDataRaw.setLegs(List.of(leg1, leg2));

        dealService.validateTwoLeggedDeal(fixDerivativeDataRaw);
    }

    @Test
    void validateTwoLeggedDeal_exception_null() {
        FixDerivativeDataRaw fixDerivativeDataRaw = new FixDerivativeDataRaw();

        assertThrows(IncorrectNumberOfLegsException.class, () -> dealService.validateTwoLeggedDeal(fixDerivativeDataRaw));
    }

    @Test
    void validateTwoLeggedDeal_exception_incorrectNumber() {
        FixDerivativeDataRaw fixDerivativeDataRaw = new FixDerivativeDataRaw();
        FixDerivativeDataRaw.Leg leg1 = new FixDerivativeDataRaw.Leg();
        fixDerivativeDataRaw.setLegs(List.of(leg1));

        assertThrows(IncorrectNumberOfLegsException.class, () -> dealService.validateTwoLeggedDeal(fixDerivativeDataRaw));
    }
}
