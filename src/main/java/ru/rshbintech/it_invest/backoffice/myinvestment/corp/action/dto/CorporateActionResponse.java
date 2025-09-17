package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import lombok.Data;
import java.util.List;

@Data
public class CorporateActionResponse {
    private List<CorporateActionNotification> data;
    private String nextId;
}