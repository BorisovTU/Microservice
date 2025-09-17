package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SendCorpActionsAssignmentReq {

    @JsonProperty("SendCorpActionsAssignmentReq")
    private SendCorpActionsAssignmentReqData sendCorpActionsAssignmentReq;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SendCorpActionsAssignmentReqData {
        @JsonProperty("CorporateActionInstruction")
        private CorporateActionInstructionData corporateActionInstruction;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CorporateActionInstructionData {
        @JsonProperty("CorporateActionIssuerID")
        private String corporateActionIssuerID;

        @JsonProperty("CorpActnEvtId")
        private String corpActnEvtId;

        @JsonProperty("CFTID")
        private String cftid;

        @JsonProperty("OwnerSecurityID")
        private String ownerSecurityID;

        @JsonProperty("InstrDt")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'")
        private OffsetDateTime instrDt;

        @JsonProperty("InstrNmb")
        private String instrNmb;

        @JsonProperty("FinInstrmId")
        private FinInstrmIdData finInstrmId;

        @JsonProperty("Acct")
        private String acct;

        @JsonProperty("SubAcct")
        private String subAcct;

        @JsonProperty("SfkpgAcct")
        private String sfkpgAcct;

        @JsonProperty("CorpActnOptnDtls")
        private CorpActnOptnDtlsData corpActnOptnDtls;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FinInstrmIdData {
        @JsonProperty("ISIN")
        private String isin;

        @JsonProperty("RegNumber")
        private String regNumber;

        @JsonProperty("NSDR")
        private String nsdr;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CorpActnOptnDtlsData {
        @JsonProperty("OptnNb")
        private String optnNb;

        @JsonProperty("OptnTp")
        private String optnTp;

        @JsonProperty("PricVal")
        private String pricVal;

        @JsonProperty("PricValCcy")
        private String pricValCcy;

        @JsonProperty("Bal")
        private String bal;
    }
}
