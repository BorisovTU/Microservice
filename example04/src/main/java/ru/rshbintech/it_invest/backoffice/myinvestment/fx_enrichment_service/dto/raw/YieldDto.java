package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.raw;

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
public class YieldDto {

    @JsonProperty("text")
    private String text;

    @JsonProperty("noMiscFees")
    private Integer noMiscFees;

    @JsonProperty("miscFeeAmt")
    private BigDecimal miscFeeAmt;

    @JsonProperty("miscFeeType")
    private Integer miscFeeType;
}
