package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.ContractNotFoundException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.FinInstrNotFoundException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.MarketSchemeNotFound;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;

import java.math.BigDecimal;

@Slf4j
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DealProcessor {
    private FixDerivativeDataRaw rawData;

    private FinancialInstrumentMoexLink financialInstrumentMoexLink;

    private FinancialInstrument financialInstrument;

    private ContractMoexLink contractMoexLink;

    private Contract contract;

    private Boolean isClientDeal;

    private CommissionPlan commissionPlan;

    private CommissionType commissionType;

    private MarketSchemeMoexLink marketSchemeMoexLink;

    private DealEnriched dealEnriched;

    private LegData legData;

    public DealProcessor(FixDerivativeDataRaw rawData) {
        this.rawData = rawData;
    }

    public DealProcessor() {
    }

    public void setLegData(String side, BigDecimal lastPrice, Long ratioQty, String instrumentCode) {
        this.legData = new LegData(side, lastPrice, ratioQty, instrumentCode);
    }

    public DealProcessor withFiInstrumentMoexLink(FinancialInstrumentMoexLink fiMoexLink) {
        if (fiMoexLink == null) {
            log.error("Financial instrument moex link not found");
            throw new FinInstrNotFoundException();
        }
        this.financialInstrumentMoexLink = fiMoexLink;
        return this;
    }

    public DealProcessor withFiInstrument(FinancialInstrument fiInstrument) {
        if (fiInstrument == null) {
            log.error("Financial instrument not found");
            throw new FinInstrNotFoundException();
        }
        this.financialInstrument = fiInstrument;
        return this;
    }

    public DealProcessor withBankMarketPlaceCode(BankMarketPlaceCode bankMarketPlaceCode) {
        this.isClientDeal = (bankMarketPlaceCode == null);
        return this;
    }

    public DealProcessor withMarketSchemeMoexLink(MarketSchemeMoexLink marketSchemeMoexLink) {
        if (marketSchemeMoexLink == null) {
            log.error("Market scheme not found");
            throw new MarketSchemeNotFound();
        }
        this.marketSchemeMoexLink = marketSchemeMoexLink;
        return this;
    }

    public DealProcessor withContractMoexLink(ContractMoexLink contractLink) {
        if (contractLink == null) {
            log.error("Contract moex link not found");
            throw new ContractNotFoundException();
        }
        this.contractMoexLink = contractLink;
        return this;
    }

    public DealProcessor withContract(Contract contract) {
        if (contract == null) {
            log.error("Contract not found");
            throw new ContractNotFoundException();
        }
        this.contract = contract;
        return this;
    }

    public DealProcessor withCommissionPlan(CommissionPlan commissionPlan) {
        if (commissionPlan == null) {
            log.warn("Not found commission plan");
        }
        this.commissionPlan = commissionPlan;
        return this;
    }

    public DealProcessor withCommissionType(CommissionType commissionType) {
        if (commissionType == null) {
            log.warn("Not found commission type");
        }
        this.commissionType = commissionType;
        return this;
    }

    /*
        Для того, чтобы сделки с одной и с двумя ногами можно было обрабатывать одинаково
        сделана эта структура. В неё сохраняются данные, которые берутся из разных мест
        для одноногой или двуногой сделки
     */
    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class LegData {
        private String side;
        private BigDecimal lastPrice;
        private Long ratioQty;
        private String instrumentCode;
        private Integer spreadNumber;

        public LegData(String side, BigDecimal lastPrice, Long ratioQty, String instrumentCode) {
            this.side = side;
            this.lastPrice = lastPrice;
            this.ratioQty = ratioQty;
            this.instrumentCode = instrumentCode;
        }

    }
}
