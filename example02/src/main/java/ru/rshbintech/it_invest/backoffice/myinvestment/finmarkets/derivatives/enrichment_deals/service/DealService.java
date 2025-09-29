package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.DrawingDateNotDeterminedException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.IncorrectNumberOfLegsException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.PriceNotDeterminedException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.globalTables.CalendarLookupTable;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@RequiredArgsConstructor
public class DealService {

    private static final String MOEX_EXCHANGE_ID = "2";
    private static final String TRADE_DEAL_KIND = "TRADE";
    public static final String FUTURES = "Фьючерс";
    private static final int DEFAULT_POINT = 5;
    private static final LocalTime CLEARING_TIME_THRESHOLD = LocalTime.of(18, 50);
    private static final DateTimeFormatter INPUT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
    private static final DateTimeFormatter OUTPUT_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final CalendarLookupTable calendarLookupTable;
    private final CommissionService commissionService;

    public DealEnriched createBaseDeal(FixDerivativeDataRaw rawData) {
        DealEnriched deal = new DealEnriched();
        LocalDateTime transactTime = LocalDateTime.parse(rawData.getTransactTime(), INPUT_DATE_FORMATTER);

        deal.setDealKind(TRADE_DEAL_KIND);
        deal.setPoint(DEFAULT_POINT);
        deal.setCounterPartyId(MOEX_EXCHANGE_ID);
        deal.setExchangeId(MOEX_EXCHANGE_ID);
        deal.setIsPrognose(true);

        deal.setExternalCode(String.valueOf(rawData.getSecondaryExecId()));
        deal.setCreatedDate(transactTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        deal.setClearingDate(determineClearingDate(transactTime));
        deal.setRequestExternalCode(String.valueOf(rawData.getOrderId()).substring(4));

        if (rawData.getComplianceId() != null) {
            deal.setIsMarginCall(rawData.getComplianceId().equals("D"));
        }
        return deal;
    }

    public void setLegSpecificFields(DealEnriched deal, String side, Double amount, Long secondaryExecId) {
        deal.setDirectionFromSide(side);
        deal.setCodeFromTimeAndIDAndDirection(deal.getDirection(), deal.getCreatedDate(), secondaryExecId);
        deal.setAmount(amount);
    }

    public void setInstrumentSpecificFields(DealEnriched deal, FinancialInstrument financialInstrument, BigDecimal basePrice) {
        deal.setFiId(String.valueOf(financialInstrument.id()));

        if (financialInstrument.pfiKind().equalsIgnoreCase(FUTURES)) {
            deal.setPrice(basePrice);
        } else { //опцион
            deal.setBonus(basePrice);
            if (financialInstrument.priceMode() != null && financialInstrument.priceMode() == 1) {
                deal.setPrice(financialInstrument.strike());
            } else {
                throw new PriceNotDeterminedException();
            }
        }
    }

    public void setPrices(DealEnriched deal, FinancialInstrument financialInstrument) {
        BigDecimal priceRUB = financialInstrument.tickCost().divide(financialInstrument.tick(), 5, RoundingMode.HALF_UP);
        priceRUB = priceRUB.multiply(deal.getPrice()).setScale(2, RoundingMode.HALF_UP);
        deal.setPriceRUB(priceRUB);

        if (financialInstrument.tickFiID() == 0) {
            deal.setPrice2(deal.getPriceRUB());
        } else {
            deal.setPrice2(deal.getPrice().multiply(financialInstrument.faceValue()));
        }

        deal.setPositionCost(deal.getPrice2().multiply(BigDecimal.valueOf(deal.getAmount())));
    }

    public void setContractSpecificFields(DealEnriched deal, Contract contract) {
        deal.setContractId(String.valueOf(contract.id()));
        deal.setClientId(String.valueOf(contract.clientID()));
    }

    public void setMarketScheme(DealEnriched deal, MarketSchemeMoexLink marketSchemeMoexLink) {
        deal.setMarketScheme(marketSchemeMoexLink.schemeId());
    }

    public void putExchangeCommission(DealEnriched deal, int commissionId, BigDecimal commissionSum) {
        Commission commission = commissionService.getExchangeCommission(commissionId, commissionSum);
        if (commission != null) {
            deal.putCommission(commission);
        }
    }

    public void putClientCommission(DealEnriched deal, CommissionType commissionType, CommissionPlan commissionPlan) {
        Commission commission = commissionService.calculateBrokerageCommission(commissionType,
                commissionPlan,
                deal.getAmount());
        if (commission != null) {
            deal.putCommission(commission);
        }
    }

    private String determineClearingDate(LocalDateTime transactTime) {
        return transactTime.toLocalTime().isBefore(CLEARING_TIME_THRESHOLD)
                ? transactTime.format(OUTPUT_DATE_FORMATTER)
                : calendarLookupTable.findNextWorkDate(transactTime.toLocalDate()).format(OUTPUT_DATE_FORMATTER);
    }

    public LocalDateTime parseDrawingDate(String drawingDate) {
        if (drawingDate == null) {
            throw new DrawingDateNotDeterminedException();
        }
        return LocalDateTime.parse(drawingDate, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public void setSpreadLegNumber(DealEnriched dealEnriched, Integer spreadLegNumber) {
        if (spreadLegNumber != null) {
            dealEnriched.setSpreadLegNumber(spreadLegNumber);
        }
    }

    public void validateTwoLeggedDeal(FixDerivativeDataRaw rawData) {
        List<FixDerivativeDataRaw.Leg> legs = rawData.getLegs();

        if (legs == null || legs.size() != 2) {
            throw new IncorrectNumberOfLegsException();
        }
    }
}
