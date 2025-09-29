package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class FixDerivativeDataRaw {

    private String clOrdId; //Пользовательский идентификатор заявки
    private Long clOrdLinkId; //Внешний номер
    private String execType; //Тип отчёта
    private String ordStatus;
    private String execId; //FIX Gate формирует уникальный ID для каждого исходящего Execution Report
    private Long orderId; //Идентификатор заявки в системе SPECTRA (для айсбергов – идентификационный номер всей айсберг-заявки)
    private String secondaryOrderId; //Дополнительный идентификатор заявки
    private Long tradingSessionId;
    private String account; //7-ми символьный код клиентского счета
    private String transactTime;
    private String symbol; //Символьный код инструмента
    private String side;
    private BigDecimal price;
    private Long orderQty; //Количество единиц инструмента (для айсбергов - количество единиц инструмента во всей айсберг-заявке).
    private Long leavesQty; //Размер остающейся к исполнению части заявки
    private Long cumQty; //Суммарное количество единиц биржевого инструмента, купленного или проданного по заявке
    private Long lastQty; //Количество единиц инструмента в сделке
    private BigDecimal avgPx; //Средняя цена сделок по заявке
    private Long ordRejReason; //Причина отклонения запроса. Указывается для Execution Report с ExecType=8 (Rejected).
    private String text;
    private Long secondaryExecId;
    private Long trdMatchId;
    private BigDecimal lastPx;
    private Long noMiscFees;
    private BigDecimal miscFeeAmt; //Сбор за сделку
    private Integer miscFeeType; //"4" (Exchange Fees)
    private Integer noLegs; //"2" Указывается для Execution Report с ExecType=F (Trade) сделок по связкам
    private List<Leg> legs;
    private String solicitedFlag;
    private String copyMsgIndicator;
    private String secondaryClOrdId;
    private Integer noPartyIds;
    private List<Party> parties;
    private String ordType;
    private String expireDate;
    private Long flags;
    private Integer execRestatementReason;
    private String nccRequest;
    private Long displayQty;
    private Long displayVarianceQty;
    private String displayMethod;
    private Long mdEntryId;
    private Long firstOrderId;
    private Long flags2;
    private String complianceId;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Party {
        private String partyId; //5-ти буквенный код участника
        private String partyIdSource; //"C", Generally accepted market participant identifier
        private Integer partyRole;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Leg {
        private String legSymbol; //Символьный код фьючерса, входящего в календарный спред
        private Long legRatioQty; //Коэффициент количества
        private String legSide;
        private BigDecimal legLastPrice;
    }
}
