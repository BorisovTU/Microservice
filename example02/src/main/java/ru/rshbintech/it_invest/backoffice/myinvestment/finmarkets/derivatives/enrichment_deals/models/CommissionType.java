package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CommissionType (
        @JsonProperty("ID")      int id,
        @JsonProperty("CODE")    String code,
        @JsonProperty("SUM_MIN") BigDecimal minSum,
        @JsonProperty("SUM_MAX") BigDecimal maxSum
){}