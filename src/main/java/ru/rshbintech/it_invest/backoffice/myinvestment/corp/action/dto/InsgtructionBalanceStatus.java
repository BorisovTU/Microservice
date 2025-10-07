package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import lombok.Getter;

@Getter
public enum InsgtructionBalanceStatus {
    ACCEPTED("ACCEPTED", null),
    OUT_OF_BALANCE("REJECTED","Недостаточно ЦБ для подачи поручения");

    private final String name;
    private final String description;
    InsgtructionBalanceStatus(final String name, final String description) {
        this.name = name;
        this.description = description;
    }

}
