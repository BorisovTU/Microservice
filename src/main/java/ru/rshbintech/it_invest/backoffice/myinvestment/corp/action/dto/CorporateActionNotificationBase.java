package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorporateActionNotificationBase {
    @JsonProperty("CorporateActionIssuerID")
    private String corporateActionIssuerID;

    @JsonProperty("CorporateActionType")
    private String corporateActionType;

    @JsonProperty("CorpActnEvtId")
    private String corpActnEvtId;

    @JsonProperty("EvtTp")
    private String evtTp;

    @JsonProperty("MndtryVlntryEvtTp")
    private String mndtryVlntryEvtTp;

    @JsonProperty("RcrdDt")
    private String rcrdDt;

    @JsonProperty("OrgNm")
    private String orgNm;

    @JsonProperty("SfkpgAcct")
    private String sfkpgAcct;

    @JsonProperty("FinInstrmId")
    private FinInstrmId finInstrmId;

    @JsonProperty("AddtlInf")
    private String addtlInf;

    @JsonProperty("LwsInPlcCd")
    private String lwsInPlcCd;

    @JsonProperty("SbrdntLwsInPlcCd")
    private String sbrdntLwsInPlcCd;

    @JsonProperty("CorpActnOptnDtls")
    private List<CorpActnOptnDtls> corpActnOptnDtls;
}
