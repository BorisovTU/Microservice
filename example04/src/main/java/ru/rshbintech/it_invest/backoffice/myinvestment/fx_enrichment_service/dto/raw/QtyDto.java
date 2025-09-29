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
public class QtyDto {

    @JsonProperty("ordType")
    private String ordType;

    @JsonProperty("price")
    private BigDecimal price;

    @JsonProperty("qty")
    private BigDecimal qty;

    @JsonProperty("timeInForce")
    private String timeInForce;

    @JsonProperty("tradingSessionID")
    private String tradingSessionID;

    @JsonProperty("leavesQty")
    private BigDecimal leavesQty;

    @JsonProperty("cumQty")
    private BigDecimal cumQty;

    @JsonProperty("avgPx")
    private BigDecimal avgPx;

    @JsonProperty("transactTime")
    private String transactTime;
}
