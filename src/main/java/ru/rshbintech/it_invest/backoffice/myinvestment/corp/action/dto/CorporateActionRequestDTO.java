package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CorporateActionRequestDTO {
    private int limit;
    private String isin;
    private Long cftid;
    private Long nextId;
    private Long caid;
    private CASEnum sort;
    private boolean status;
    private String mndtryVlntryEvtTp;

}
