package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CorporateActionNotificationDto {
    @JsonProperty("CorporateActionNotification")
    private CorporateActionNotification CorporateActionNotification;
}
