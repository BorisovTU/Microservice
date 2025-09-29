package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubcontractDto {

    @JsonProperty("ID")
    private String id;

    @JsonProperty("CLIENT_ID")
    private String clientId;

    @JsonProperty("PLAN_ID")
    private String planId;
}
