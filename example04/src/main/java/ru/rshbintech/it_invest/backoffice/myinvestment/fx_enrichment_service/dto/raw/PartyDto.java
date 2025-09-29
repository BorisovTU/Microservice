package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PartyDto {

    @JsonProperty("execID")
    private String execID;

    @JsonProperty("execType")
    private String execType;

    @JsonProperty("ordStatus")
    private String ordStatus;

    @JsonProperty("workingIndicator")
    private Boolean workingIndicator;

    @JsonProperty("ordRejReason")
    private Integer ordRejReason;

    @JsonProperty("execRestatementReason")
    private String execRestatementReason;

    @JsonProperty("account")
    private String account;

    @JsonProperty("settlDate")
    private String settlDate;
}
