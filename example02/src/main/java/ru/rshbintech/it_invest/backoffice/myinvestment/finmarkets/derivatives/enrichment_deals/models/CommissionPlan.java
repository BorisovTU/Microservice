package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
public record CommissionPlan (
    @JsonProperty("ID")                    String id,
    @JsonProperty("COMISS_ID")             int commissionID,
    @JsonProperty("PLAN_ID")               int planID,
    @JsonProperty("MIN_BASE")              Double minBase,
    @JsonProperty("MAX_BASE")              Double maxBase,
    @JsonProperty("IS_INCLUSIVE_MIN_BASE") boolean isInclusiveMinBase,
    @JsonProperty("IS_INCLUSIVE_MAX_BASE") boolean isInclusiveMaxBase,
    @JsonProperty("BASE_TYPE")             String baseType,
    @JsonProperty("TARIFF_SUM")            Double tariffSum
) {}