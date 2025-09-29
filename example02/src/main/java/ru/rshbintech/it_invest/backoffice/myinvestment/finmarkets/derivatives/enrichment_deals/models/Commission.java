package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import java.math.BigDecimal;

public record Commission (
        String commissionId, // Идентификатор вида комиссии
        BigDecimal sum, // Сумма комиссии
        double nds // НДС комиссии (0)
) {}