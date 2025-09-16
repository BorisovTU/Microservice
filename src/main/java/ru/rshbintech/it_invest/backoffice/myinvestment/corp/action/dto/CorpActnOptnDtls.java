package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorpActnOptnDtls {
    @JsonProperty("OptnNb")
    private String optnNb;

    @JsonProperty("OptnTp")
    private String optnTp;

    @JsonProperty("DfltOptnInd")
    private String dfltOptnInd;

    @JsonProperty("PricVal")
    private String pricVal;

    @JsonProperty("PricValCcy")
    private String pricValCcy;

    @JsonProperty("ActnPrd")
    private ActnPrd actnPrd;
}
