package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.OffsetDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorporateActionInstructionRequest {

    @NotBlank(message = "InstrNmb is required")
    @JsonProperty("InstrNmb")
    private String instrNmb;

    @NotNull(message = "InstrDt is required")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    @JsonProperty("InstrDt")
    private OffsetDateTime instrDt;

    @NotNull(message = "Bal is required")
    @JsonProperty("Bal")
    private Number bal;

    @Valid
    @NotNull(message = "BnfclOwnrDtls is required")
    @JsonProperty("BnfclOwnrDtls")
    private BnfclOwnrDtlsRequest bnfclOwnrDtls;

    @Valid
    @NotNull(message = "CorpActnOptnDtls is required")
    @JsonProperty("CorpActnOptnDtls")
    private CorpActnOptnDtlsRequest corpActnOptnDtls;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BnfclOwnrDtlsRequest {
        @NotBlank(message = "OwnerSecurityID is required")
        @JsonProperty("OwnerSecurityID")
        private String ownerSecurityID;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CorpActnOptnDtlsRequest {
        @NotBlank(message = "OptnNb is required")
        @JsonProperty("OptnNb")
        private String optnNb;
    }
}
