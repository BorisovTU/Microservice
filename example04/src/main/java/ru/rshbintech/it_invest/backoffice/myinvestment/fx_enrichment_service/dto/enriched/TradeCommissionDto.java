package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.dto.enriched;

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
public class TradeCommissionDto {

    @JsonProperty("commissionId")
    private String commissionId;

    @JsonProperty("sum")
    private BigDecimal sum;

    @JsonProperty("nds")
    private BigDecimal nds;
}
