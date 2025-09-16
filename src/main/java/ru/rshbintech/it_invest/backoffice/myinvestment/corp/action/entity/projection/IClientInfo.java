package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.projection;

public interface IClientInfo {
    Long getOwnerSecurityID();
    Long getCftid();
    String getAccDepo();
    String getSubAccDepo();
    Long getBal();
}
