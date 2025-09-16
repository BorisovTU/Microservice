package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class CorporateActionResponse {
    private List<CorporateActionNotificationResponseDTO> data;
    private String nextId;
}
