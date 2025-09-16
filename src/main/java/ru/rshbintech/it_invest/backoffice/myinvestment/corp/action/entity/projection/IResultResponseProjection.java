package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection;

public interface IResultResponseProjection {
    Long getCaid();
    String getReference();
    String getCftid();
    String getIsin();
    String getRegNumber();
    String getNsdr();
    String getAccDepo();
    String getSubAccDepo();
    String getSfkpgAcct();
}
