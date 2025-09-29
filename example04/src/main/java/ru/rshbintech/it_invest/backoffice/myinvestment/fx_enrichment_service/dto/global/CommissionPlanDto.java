package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.global;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommissionPlanDto {

    @JsonProperty("ID")
    private String id;

    @JsonProperty("COMMISSION_ID")
    private String commissionId;

    @JsonProperty("MIN_BASE")
    private BigDecimal minBase;

    @JsonProperty("IS_INCLUSIVE_MIN_BASE")
    private Boolean inclusiveMinBase;

    @JsonProperty("TARIFF_SUM")
    private BigDecimal tariffSum;

    @JsonProperty("MIN_SUM")
    private BigDecimal minValue;

    @JsonProperty("MAX_SUM")
    private BigDecimal maxValue;
}
