package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service_sofr.dto;

import java.math.BigDecimal;

public record TradeCommissionDto(Integer commissionId,
                                 BigDecimal sum,
                                 BigDecimal nds) {
}
