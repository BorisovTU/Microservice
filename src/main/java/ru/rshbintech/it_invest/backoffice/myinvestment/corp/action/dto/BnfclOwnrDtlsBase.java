package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class BnfclOwnrDtlsBase {
    @JsonProperty("OwnerSecurityID")
    private String ownerSecurityID;

    @JsonProperty("Acct")
    private String acct;

    @JsonProperty("SubAcct")
    private String subAcct;

    @JsonProperty("Bal")
    private String bal;
}