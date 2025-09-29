package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.kafka.processors;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.ContractNotFoundException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.exception.FinInstrNotFoundException;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.*;

@Data
@Slf4j
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RequestProcessor {
    private FixDerivativeDataRaw rawData;

    private FinancialInstrumentMoexLink financialInstrumentMoexLink;

    private FinancialInstrument financialInstrument;

    private ContractMoexLink contractMoexLink;

    private Contract contract;

    private Boolean isClientRequest;

    private RequestEnriched requestEnriched;

    public RequestProcessor(FixDerivativeDataRaw rawData) {
        this.rawData = rawData;
    }

    public RequestProcessor() {
    }

    public RequestProcessor withFiInstrumentMoexLink(FinancialInstrumentMoexLink fiMoexLink) {
        if (fiMoexLink == null) {
            log.error("Financial moex link instrument not found");
            throw new FinInstrNotFoundException();
        }
        this.financialInstrumentMoexLink = fiMoexLink;
        return this;
    }

    public RequestProcessor withFiInstrument(FinancialInstrument fiInstrument) {
        if (fiInstrument == null) {
            log.error("Financial instrument not found");
            throw new FinInstrNotFoundException();
        }
        this.financialInstrument = fiInstrument;
        return this;
    }

    public RequestProcessor withContractMoexLink(ContractMoexLink contractLink) {
        if (contractLink == null) {
            log.error("Contract moex link not found");
            throw new ContractNotFoundException();
        }
        this.contractMoexLink = contractLink;
        return this;
    }

    public RequestProcessor withContract(Contract contract) {
        if (contract == null) {
            log.error("Contract not found");
            throw new ContractNotFoundException();
        }
        this.contract = contract;
        return this;
    }

    public RequestProcessor withBankMarketPlaceCode(BankMarketPlaceCode bankMarketPlaceCode) {
        this.isClientRequest = (bankMarketPlaceCode == null);
        return this;
    }
}
