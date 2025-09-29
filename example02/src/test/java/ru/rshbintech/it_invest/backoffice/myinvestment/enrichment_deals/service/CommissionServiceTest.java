package ru.rshbintech.it_invest.backoffice.myinvestment.enrichment_deals.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.Commission;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.CommissionPlan;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.CommissionType;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service.CommissionService;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class CommissionServiceTest {

    @InjectMocks
    private CommissionService commissionService;

    private CommissionType createCommissionType() {
        return new CommissionType(1, "code", BigDecimal.valueOf(2000.0), BigDecimal.valueOf(15000.0));
    }

    private CommissionPlan createCommissionPlan() {
        CommissionPlan commissionPlan = new CommissionPlan("COMMISSION_ID",
                1,
                0,
                150.0,
                0.0,
                true,
                false,
                "QTY",
                10.0);

        return commissionPlan;
    }

    @Test
    void createCommission() {
        Commission commission = commissionService.createCommission(1, BigDecimal.valueOf(2.0));

        assertEquals(new Commission("1", BigDecimal.valueOf(2.0), 0.0), commission);
    }

    @Test
    void getExchangeCommission_success() {
        Commission commission = commissionService.getExchangeCommission(1, BigDecimal.valueOf(2.0));

        assertEquals(new Commission("1", BigDecimal.valueOf(2.0), 0.0), commission);
    }

    @Test
    void getExchangeCommission_fail_null() {
        Commission commission = commissionService.getExchangeCommission(1, null);

        assertNull(commission);
    }

    @Test
    void getExchangeCommission_fail_zero() {
        Commission commission = commissionService.getExchangeCommission(1, BigDecimal.valueOf(0.0));

        assertNull(commission);
    }

    @Test
    void calculateBrokerageCommission_success() {
        CommissionType commissionType = createCommissionType();
        CommissionPlan commissionPlan = createCommissionPlan();

        Commission commission = commissionService.calculateBrokerageCommission(commissionType, commissionPlan, 1000.0);

        assertEquals(new Commission("1", BigDecimal.valueOf(10000.0), 0.0), commission);
    }

    @Test
    void calculateBrokerageCommission_success_AmountLessThenMinBase() {
        CommissionPlan commissionPlan = createCommissionPlan();
        CommissionType commissionType = createCommissionType();

        Commission commission = commissionService.calculateBrokerageCommission(commissionType, commissionPlan, 10.0);

        assertNull(commission);
    }

    @Test
    void calculateBrokerageCommission_success_AmountSetMinimalValue() {
        CommissionPlan commissionPlan = createCommissionPlan();
        CommissionType commissionType = createCommissionType();

        Commission commission = commissionService.calculateBrokerageCommission(commissionType, commissionPlan, 180.0);

        assertEquals(new Commission("1", BigDecimal.valueOf(2000.0), 0.0), commission);
    }

    @Test
    void calculateBrokerageCommission_success_AmountSetMaximumValue() {
        CommissionPlan commissionPlan = createCommissionPlan();
        CommissionType commissionType = createCommissionType();

        Commission commission = commissionService.calculateBrokerageCommission(commissionType, commissionPlan, 3000.0);

        assertEquals(new Commission("1", BigDecimal.valueOf(15000.0), 0.0), commission);
    }
}
