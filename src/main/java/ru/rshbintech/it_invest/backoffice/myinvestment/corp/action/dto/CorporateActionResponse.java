package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import lombok.Data;
import java.util.List;

@Data
public class CorporateActionResponse {
    private List<CorporateActionNotificationDto> data;
    private String nextId;
}