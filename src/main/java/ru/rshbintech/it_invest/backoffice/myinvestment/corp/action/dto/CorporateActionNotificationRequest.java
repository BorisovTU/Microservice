package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorporateActionNotificationRequest extends CorporateActionNotificationBase {
    @JsonProperty("BnfclOwnrDtls")
    private List<BnfclOwnrDtlsRequest> bnfclOwnrDtls;
}
