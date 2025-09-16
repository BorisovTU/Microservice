package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.time.OffsetDateTime;

@Data
public class CorporateActionInstruction {
    private String instrNmb;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    private OffsetDateTime instrDt;

    private Number bal;
    private String status;
    private CorporateActionNotificationShort corporateActionNotification;
    private BnfclOwnrDtlsShort bnfclOwnrDtls;
    private CorpActnOptnDtlsRequest corpActnOptnDtls;

    @Data
    public static class CorporateActionNotificationShort {
        private String corporateActionType;
        private String corpActnEvtId;
        private String evtTp;
        private String mndtryVlntryEvtTp;
        private CorporateActionNotification.FinInstrmId finInstrmId;
        private CorporateActionNotification.RspnPrd actnPrd;
        private String addtlInf;
    }

    @Data
    public static class BnfclOwnrDtlsShort {
        private String ownerSecurityID;
        private String cftid;
        private String acct;
        private String subAcct;
    }

    @Data
    public static class CorpActnOptnDtlsRequest {
        private String optnNb;
    }
}
