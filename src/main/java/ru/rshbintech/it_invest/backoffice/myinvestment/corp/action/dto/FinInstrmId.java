package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class FinInstrmId {
    @JsonProperty("ISIN")
    private String isin;

    @JsonProperty("RegNumber")
    private String regNumber;

    @JsonProperty("NSDR")
    private String nsdr;

}