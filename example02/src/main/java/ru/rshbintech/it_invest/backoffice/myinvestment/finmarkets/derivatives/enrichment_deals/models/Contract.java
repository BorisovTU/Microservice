package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record Contract(
        @JsonProperty("ID") long id,
        @JsonProperty("MARKETPLACE_CODE") String marketplaceCode,
        @JsonProperty("CLIENT_ID") Long clientID,
        @JsonProperty("FIRM_ID") String firmID,
        @JsonProperty("PLAN_ID") Integer planID
) {}