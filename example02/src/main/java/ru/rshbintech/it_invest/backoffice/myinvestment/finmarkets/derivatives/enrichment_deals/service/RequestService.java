package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.service;

import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.Contract;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.FinancialInstrument;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.FixDerivativeDataRaw;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.RequestEnriched;
import ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.enums.ExecTypes;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Component
public class RequestService {
    private static final String MOEX_EXCHANGE_ID = "2";
    public static final String FUTURES = "Фьючерс";
    public static final String INDEX = "Индекс";
    public static final String POINTS = "Пункты";
    public static final String CURRENCY = "Валюта";

    public RequestEnriched createBaseRequest(FixDerivativeDataRaw fixDerivativeDataRaw) {
        RequestEnriched request = new RequestEnriched();

        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS");
        LocalDateTime transactTime = LocalDateTime.parse(fixDerivativeDataRaw.getTransactTime(), inputFormatter);

        request.setExternalCode(String.valueOf(fixDerivativeDataRaw.getOrderId()).substring(4));
        request.setDirectionFromSide(fixDerivativeDataRaw.getSide());
        request.setAmount(fixDerivativeDataRaw.getOrderQty());
        request.setPrice(fixDerivativeDataRaw.getPrice());
        request.setStatus(ExecTypes.getNameByCode(fixDerivativeDataRaw.getExecType()));
        request.setLastUpdateTime(transactTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));

        if (fixDerivativeDataRaw.getFlags() != null) {
            request.setIsAddress((fixDerivativeDataRaw.getFlags() & (0x4000000 | 0x80000000)) != 0);
        }

        if (fixDerivativeDataRaw.getComplianceId() != null) {
            request.setIsMarginCall(fixDerivativeDataRaw.getComplianceId().equals("D"));
        }

        if (ExecTypes.NEW.getCode().equals(fixDerivativeDataRaw.getExecType())) {
            request.setCodeFromTransactTimeAndOrderId(transactTime, String.valueOf(fixDerivativeDataRaw.getOrderId()));
            request.setCreatedDate(transactTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        }

        request.setExchangeId(MOEX_EXCHANGE_ID);

        return request;
    }

    public void setInstrumentSpecificFields(RequestEnriched request, FinancialInstrument financialInstrument) {
        request.setFiId(String.valueOf(financialInstrument.id()));

        if (FUTURES.equalsIgnoreCase(financialInstrument.pfiKind())
                || FUTURES.equalsIgnoreCase(financialInstrument.faceValueKind())
                || INDEX.equalsIgnoreCase(financialInstrument.faceValueKind())) {
            request.setPriceType(POINTS);
        } else {
            request.setPriceType(CURRENCY);
            request.setPriceFiId(String.valueOf(financialInstrument.rateFiID()));
        }
    }

    public void setContractSpecificFields(RequestEnriched request, Contract contract) {
        request.setContractId(String.valueOf(contract.id()));
        request.setClientId(String.valueOf(contract.clientID()));
    }
}
