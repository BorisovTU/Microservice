package ru.rshbintech.it_invest.backoffice.myinvestment.fx_client_trades_service.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record TradeDto(Integer tradeKind,
                       String code,
                       String externalCode,
                       String direction,
                       LocalDateTime createdDate,
                       Long contractId,
                       Long clientId,
                       Long fiId,
                       BigDecimal amount,
                       BigDecimal price,
                       LocalDateTime clearingDate,
                       Integer counterpartyId,
                       Long exchangeId,
                       Long orderId,
                       Integer marketScheme,
                       List<TradeCommissionDto> commissions) {
}
