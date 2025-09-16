package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

@Data
public class SendCorpActionsAssignmentReqDTO {
    @NotNull
    @Valid
    @JsonProperty("CorporateActionInstruction")
    private CorporateActionInstructionDTO corporateActionInstruction;

    @Data
    public static class CorporateActionInstructionDTO {
        @NotBlank
        private String corporateActionIssuerID;

        @NotBlank
        private String corpActnEvtId;

        @NotBlank
        private String CFTID;

        @NotBlank
        private String ownerSecurityID;

        @NotBlank
        @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$")
        private String instrDt;

        @NotBlank
        private String instrNmb;

        @NotNull
        @Valid
        private FinInstrmIdDTO finInstrmId;

        @NotBlank
        private String acct;

        @NotBlank
        private String subAcct;

        @NotBlank
        private String sfkpgAcct;

        @NotNull
        @Valid
        private CorpActnOptnDtlsDTO corpActnOptnDtls;
    }

    @Data
    public static class FinInstrmIdDTO {
        private String ISIN;
        private String regNumber;
        private String NSDR;
    }

    @Data
    public static class CorpActnOptnDtlsDTO {
        @NotBlank
        private String optnNb;

        @NotBlank
        private String optnTp;

        private String pricVal;
        private String pricValCcy;

        @NotBlank
        private String bal;
    }
}
