package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record FinancialInstrument(

    @NotNull
    @JsonProperty("ID")                long id,
    @JsonProperty("MARKETPLACE_CODE")  String marketplaceCode,
    @JsonProperty("PFI_KIND")          String pfiKind,
    @JsonProperty("FACEVALUE")         BigDecimal faceValue,
    @JsonProperty("FACEVALUE_FI_KIND") String faceValueKind,
    @JsonProperty("RATE_FIID")         Long rateFiID,
    @JsonProperty("PRICEMODE")         Integer priceMode,
    @JsonProperty("STRIKE")            BigDecimal strike,
    @JsonProperty("TICK_FIID")         Long tickFiID,
    @JsonProperty("TICK")              BigDecimal tick,
    @JsonProperty("DRAWING_DATE")      String drawingDate,
    @JsonProperty("TICK_COST")         BigDecimal tickCost
) {}