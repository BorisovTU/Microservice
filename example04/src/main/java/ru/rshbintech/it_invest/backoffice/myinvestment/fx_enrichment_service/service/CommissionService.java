package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.CommissionPlanDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.kafka.globalTables.CommissionsGlobalTables;

import java.math.BigDecimal;

@Service
@RequiredArgsConstructor
@Slf4j
public class CommissionService {

    private static final String EXCHANGE_COMMISSION_CODE = "МскБиржПИ";
    private static final String BROKER_COMMISSION_CODE = "КлиентПФИ";

    private final CommissionsGlobalTables commissionsGlobalTables;

    public BigDecimal calculateExchangeCommission(BigDecimal amount, String contractId) {
        log.info("Начало расчёта биржевой комиссии. contractId={}, amount={}", contractId, amount);

        CommissionPlanDto plan = commissionsGlobalTables.getPlanByContractId(contractId, EXCHANGE_COMMISSION_CODE);
        if (plan == null) {
            log.warn("План для биржевой комиссии не найден. contractId={}", contractId);
            return BigDecimal.ZERO;
        }

        log.info("Биржевая комиссия рассчитана: {}", amount);
        return amount;
    }

    public BigDecimal calculateBrokerCommission(BigDecimal amount, String contractId) {
        log.info("Начало расчёта брокерской комиссии. contractId={}, amount={}", contractId, amount);

        CommissionPlanDto plan = commissionsGlobalTables.getPlanByContractId(contractId, BROKER_COMMISSION_CODE);
        if (plan == null) {
            log.warn("План для брокерской комиссии не найден. contractId={}", contractId);
            return BigDecimal.ZERO;
        }
        if (amount == null) {
            log.warn("Amount равен null, расчёт комиссии невозможен. contractId={}", contractId);
            return BigDecimal.ZERO;
        }

        BigDecimal minBase = plan.getMinBase() != null ? plan.getMinBase() : BigDecimal.ZERO;
        BigDecimal maxValue = plan.getMaxValue();
        BigDecimal tariff = plan.getTariffSum() != null ? plan.getTariffSum() : BigDecimal.ZERO;
        BigDecimal minValue = plan.getMinValue() != null ? plan.getMinValue() : BigDecimal.ZERO;
        boolean inclusive = plan.getInclusiveMinBase() != null && plan.getInclusiveMinBase();

        BigDecimal sum = BigDecimal.ZERO;

        if ((inclusive && amount.compareTo(minBase) >= 0) || (!inclusive && amount.compareTo(minBase) > 0)) {
            sum = amount.multiply(tariff);
            log.debug("Raw комиссия по тарифу: amount={} * tariff={} = {}", amount, tariff, sum);
        } else {
            log.debug("Amount меньше minBase={}, сумма комиссии = 0", minBase);
        }

        if (sum.compareTo(minValue) < 0) {
            log.debug("Сумма {} меньше minValue={}, применяется minValue", sum, minValue);
            sum = minValue;
        }
        if (maxValue != null && sum.compareTo(maxValue) > 0) {
            log.debug("Сумма {} больше maxValue={}, применяется maxValue", sum, maxValue);
            sum = maxValue;
        }

        log.info("Брокерская комиссия рассчитана: {}", sum);
        return sum;
    }
}