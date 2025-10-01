package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Data;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorporateActionInstructionRequest {

    @NotNull(message = "InstrNmb is required")
    @Pattern(regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
            message = "InstrNmb must be a valid UUID")
    @JsonProperty("InstrNmb")
    private String instrNmb;

    @NotNull(message = "InstrDt is required")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ssXXX")
    @JsonProperty("InstrDt")
    private OffsetDateTime instrDt;

    @NotNull(message = "Bal is required")
    @JsonProperty("Bal")
    private BigDecimal bal;

    @Valid
    @NotNull(message = "BnfclOwnrDtls is required")
    @JsonProperty("BnfclOwnrDtls")
    private BnfclOwnrDtlsRequest bnfclOwnrDtls;

    @Valid
    @NotNull(message = "CorpActnOptnDtls is required")
    @JsonProperty("CorpActnOptnDtls")
    private CorpActnOptnDtlsReq corpActnOptnDtls;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class BnfclOwnrDtlsRequest {
        @NotBlank(message = "OwnerSecurityID is required")
        @JsonProperty("OwnerSecurityID")
        private String ownerSecurityID;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CorpActnOptnDtlsReq {
        @NotBlank(message = "OptnNb is required")
        @JsonProperty("OptnNb")
        private String optnNb;
    }
}
