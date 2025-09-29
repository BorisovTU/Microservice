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
public class InstrumentDto {

    @JsonProperty("symbol")
    private String symbol;

    @JsonProperty("FI_KIND")
    private String definition;

    @JsonProperty("RATE_FIID")
    private String rateFiId;

    @JsonProperty("side")
    private Integer side;
}
