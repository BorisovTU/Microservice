package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection;

import java.time.LocalDate;

public interface ICorporateActionProjection {
    Long getCaid();
    String getCatype();
    String getReference();
    String getSwiftType();
    String getMaVoCode();
    LocalDate getDateRegOwners();
    String getIsin();
    LocalDate getDateStart();
    LocalDate getDateEnd();
    String getSbrdntLwsInPlcCd();
    String getLwsInPlcCd();
    String getAddtlInf();
    String getRegnumber();
    String getNsdr();
    String getOrgNm();
    String getSfkpgAcct();
}
