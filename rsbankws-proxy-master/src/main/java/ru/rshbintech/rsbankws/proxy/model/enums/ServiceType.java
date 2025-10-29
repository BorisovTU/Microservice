package ru.rshbintech.rsbankws.proxy.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum ServiceType {

    RS_BANK_WS_PROXY("RSBankWSProxy"), RS_BANK_WS("RSBankWS");

    private final String name;

}
