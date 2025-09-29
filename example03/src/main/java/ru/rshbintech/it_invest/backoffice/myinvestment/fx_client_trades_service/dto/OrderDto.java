package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record OrderDto(String code,
                       String externalCode,
                       LocalDateTime createdDate,
                       Long contractId,
                       Long clientId,
                       String direction,
                       Long fiId,
                       BigDecimal amount,
                       BigDecimal price,
                       String priceType,
                       Long priceFiId,
                       Integer statusId,
                       Integer counterpartyId,
                       Long exchangeId,
                       Integer orderMethodsId,
                       LocalDateTime lastUpdateTime) {
}
