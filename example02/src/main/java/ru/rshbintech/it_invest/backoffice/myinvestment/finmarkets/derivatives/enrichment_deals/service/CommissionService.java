package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service;

import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.Commission;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.CommissionPlan;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.CommissionType;

import java.math.BigDecimal;

@Service
public class CommissionService {

    public Commission createCommission(int commissionID, BigDecimal sum) {
        return new Commission(String.valueOf(commissionID), sum, 0.0);
    }

    public Commission getExchangeCommission(int commissionId, BigDecimal sum) {
        if (sum != null && sum.compareTo(BigDecimal.ZERO) != 0) {
            return createCommission(commissionId, sum);
        }
        return null;
    }

    //amount - количество актива
    public Commission calculateBrokerageCommission(CommissionType commissionType, CommissionPlan commissionPlan, Double amount) {
        if (commissionType == null || commissionPlan == null) {
            return null;
        }

        BigDecimal sum = BigDecimal.valueOf(0);
        //нет проверки на maxBase, потому что пока таких комиссиий нет
        if ("QTY".equalsIgnoreCase(commissionPlan.baseType())) {
            if (amount < commissionPlan.minBase() || (amount.equals(commissionPlan.minBase()) && commissionPlan.isInclusiveMinBase())) {
                sum = BigDecimal.valueOf(0);
            } else {
                sum = BigDecimal.valueOf(amount * commissionPlan.tariffSum());
            }
        }

        if (sum.compareTo(BigDecimal.ZERO) == 0) {
            return null;
        }

        sum = validateByMinMaxSum(sum, commissionType.minSum(), commissionType.maxSum());

        return createCommission(commissionType.id(), sum);
    }

    private BigDecimal validateByMinMaxSum(BigDecimal sum, BigDecimal minSum, BigDecimal maxSum) {
        if (minSum != null && minSum.compareTo(BigDecimal.ZERO) != 0 && sum.compareTo(minSum) == -1 ) {
            sum = minSum;
        }

        if (maxSum != null && maxSum.compareTo(BigDecimal.ZERO) != 0 && sum.compareTo(maxSum) == 1 ) {
            sum = maxSum;
        }

        return sum;
    }
}
