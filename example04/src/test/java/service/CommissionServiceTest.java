package service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionPlanDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.CommissionsGlobalTables;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service.CommissionService;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CommissionServiceTest {

    @Mock
    private CommissionsGlobalTables commissionsGlobalTables;
    @InjectMocks
    private CommissionService commissionService;

    private static final String CONTRACT_ID = "contract-123";

    @Test
    void calculateExchangeCommission_shouldReturnAmount_whenPlanExists() {
        BigDecimal amount = BigDecimal.valueOf(1000);
        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "МскБиржПИ"))
                .thenReturn(new CommissionPlanDto());

        BigDecimal result = commissionService.calculateExchangeCommission(amount, CONTRACT_ID);

        assertEquals(amount, result);
        verify(commissionsGlobalTables).getPlanByContractId(CONTRACT_ID, "МскБиржПИ");
    }

    @Test
    void calculateExchangeCommission_shouldReturnZero_whenPlanNotFound() {
        BigDecimal amount = BigDecimal.valueOf(1000);
        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "МскБиржПИ"))
                .thenReturn(null);

        BigDecimal result = commissionService.calculateExchangeCommission(amount, CONTRACT_ID);

        assertEquals(BigDecimal.ZERO, result);
    }

    @Test
    void calculateBrokerCommission_shouldReturnZero_whenPlanNotFound() {
        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "КлиентПФИ"))
                .thenReturn(null);

        BigDecimal result = commissionService.calculateBrokerCommission(BigDecimal.valueOf(1000), CONTRACT_ID);

        assertEquals(BigDecimal.ZERO, result);
    }

    @Test
    void calculateBrokerCommission_shouldReturnZero_whenAmountIsNull() {
        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "КлиентПФИ"))
                .thenReturn(new CommissionPlanDto());

        BigDecimal result = commissionService.calculateBrokerCommission(null, CONTRACT_ID);

        assertEquals(BigDecimal.ZERO, result);
    }

    @Test
    void calculateBrokerCommission_shouldApplyMinBase_andTariff() {
        CommissionPlanDto plan = CommissionPlanDto.builder()
                .minBase(BigDecimal.valueOf(10))
                .tariffSum(BigDecimal.valueOf(2))
                .inclusiveMinBase(true)
                .minValue(BigDecimal.ZERO)
                .maxValue(BigDecimal.valueOf(1000))
                .build();

        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "КлиентПФИ"))
                .thenReturn(plan);

        BigDecimal amount = BigDecimal.valueOf(20);
        BigDecimal result = commissionService.calculateBrokerCommission(amount, CONTRACT_ID);

        assertEquals(amount.multiply(BigDecimal.valueOf(2)), result);
    }

    @Test
    void calculateBrokerCommission_shouldApplyMinValue_whenSumLessThanMinValue() {
        CommissionPlanDto plan = CommissionPlanDto.builder()
                .minBase(BigDecimal.valueOf(10))
                .tariffSum(BigDecimal.valueOf(0.5))
                .inclusiveMinBase(true)
                .minValue(BigDecimal.valueOf(50))
                .maxValue(BigDecimal.valueOf(1000))
                .build();

        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "КлиентПФИ")).thenReturn(plan);

        BigDecimal amount = BigDecimal.valueOf(10);
        BigDecimal result = commissionService.calculateBrokerCommission(amount, CONTRACT_ID);

        assertEquals(BigDecimal.valueOf(50), result);
    }

    @Test
    void calculateBrokerCommission_shouldApplyMaxValue_whenSumExceedsMaxValue() {
        CommissionPlanDto plan = CommissionPlanDto.builder()
                .minBase(BigDecimal.valueOf(10))
                .tariffSum(BigDecimal.valueOf(10))
                .inclusiveMinBase(true)
                .minValue(BigDecimal.ZERO)
                .maxValue(BigDecimal.valueOf(100))
                .build();

        when(commissionsGlobalTables.getPlanByContractId(CONTRACT_ID, "КлиентПФИ"))
                .thenReturn(plan);

        BigDecimal amount = BigDecimal.valueOf(20);
        BigDecimal result = commissionService.calculateBrokerCommission(amount, CONTRACT_ID);

        assertEquals(BigDecimal.valueOf(100), result);
    }
}
