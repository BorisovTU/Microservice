package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.TradeClientEnrichedDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched.TradeCommissionDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.MarketSchemeDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global.SubcontractDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw.RawTradeDto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class TradeService {

    private static final String MOEX_EXCHANGE_ID = "2";

    private final CommissionService commissionService;
    private final StateStoreService stateStoreService;

    private static final DateTimeFormatter FIX_TS = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
    private static final DateTimeFormatter OUT_INSTANT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter DDMMYYYY = DateTimeFormatter.ofPattern("ddMMyyyy");

    public TradeClientEnrichedDto enrichTrade(RawTradeDto rawTrade) {
        TradeClientEnrichedDto trade = new TradeClientEnrichedDto();

        LocalDateTime transactTime = rawTrade.getQtyData() != null && !rawTrade.getQtyData().isEmpty()
                && rawTrade.getQtyData().get(0).getTransactTime() != null
                ? LocalDateTime.parse(rawTrade.getQtyData().get(0).getTransactTime(), FIX_TS)
                : null;

        if (transactTime != null) {
            trade.setCreatedDate(transactTime.format(OUT_INSTANT));
        }

        SubcontractDto subcontract = rawTrade.getParties() != null && !rawTrade.getParties().isEmpty()
                ? stateStoreService.getSubcontract(rawTrade.getParties().get(0).getAccount())
                : null;

        if (subcontract != null) {
            trade.setContractId(String.valueOf(subcontract.getId()));
            trade.setClientId(subcontract.getClientId());

            if (subcontract.getId() != null) {
                MarketSchemeDto scheme = stateStoreService.getMarketScheme(subcontract.getId());
                if (scheme != null) {
                    trade.setMarketSchemeId(scheme.getMarketSchemeId());
                }
            }
            }

        if (rawTrade.getInstrument() != null && !rawTrade.getInstrument().isEmpty()) {
            trade.setFiId(rawTrade.getInstrument().get(0).getSymbol());
        }

        if (rawTrade.getInstrument() != null && !rawTrade.getInstrument().isEmpty()) {
            int side = rawTrade.getInstrument().get(0).getSide() != null ? rawTrade.getInstrument().get(0).getSide() : 0;
            trade.setDirection(side == 1 ? "BUY" : side == 2 ? "SELL" : null);
        }

        if (rawTrade.getQtyData() != null && !rawTrade.getQtyData().isEmpty()) {
            BigDecimal lastQty = rawTrade.getQtyData().get(0).getQty() != null
                    ? rawTrade.getQtyData().get(0).getQty()
                    : BigDecimal.ZERO;
            trade.setAmount(lastQty);

            BigDecimal lastPx = rawTrade.getQtyData().get(0).getPrice() != null
                    ? rawTrade.getQtyData().get(0).getPrice()
                    : BigDecimal.ZERO;
            trade.setPrice(lastPx);
        }

        trade.setExchangeId(MOEX_EXCHANGE_ID);
        trade.setCounterPartyId(MOEX_EXCHANGE_ID);

        if (rawTrade.getParties() != null && !rawTrade.getParties().isEmpty()) {
            trade.setClearingDate(rawTrade.getParties().get(0).getSettlDate());
        }

        trade.setExternalOrderId(rawTrade.getOrderId() != null && rawTrade.getOrderId().length() > 4
                ? rawTrade.getOrderId().substring(4)
                : rawTrade.getOrderId());

        int side = rawTrade.getInstrument() != null && !rawTrade.getInstrument().isEmpty()
                && rawTrade.getInstrument().get(0).getSide() != null
                ? rawTrade.getInstrument().get(0).getSide()
                : 0;

        trade.setCode(buildTradeCode(side, transactTime, rawTrade.getOrderId()));

        BigDecimal lastQty = trade.getAmount() != null ? trade.getAmount() : BigDecimal.ZERO;
        List<TradeCommissionDto> commissions = new ArrayList<>();
        if (subcontract != null) {
            BigDecimal exchangeCommission = commissionService.calculateExchangeCommission(lastQty, String.valueOf(subcontract.getId()));
            BigDecimal brokerCommission = commissionService.calculateBrokerCommission(lastQty, String.valueOf(subcontract.getId()));

            commissions.add(TradeCommissionDto.builder()
                    .commissionId("МскБиржПИ")
                    .sum(exchangeCommission)
                    .nds(BigDecimal.ZERO)
                    .build());

            commissions.add(TradeCommissionDto.builder()
                    .commissionId("КлиентПФИ")
                    .sum(brokerCommission)
                    .nds(BigDecimal.ZERO)
                    .build());
        }
        trade.setCommissions(commissions);

        return trade;
    }

    private String buildTradeCode(int side, LocalDateTime transactTime, String orderId) {
        if (transactTime == null || orderId == null) return null;

        return (side == 1 ? "B/" : side == 2 ? "S/" : "") +
                transactTime.format(DDMMYYYY) +
                "0" +
                orderId;
    }
}
