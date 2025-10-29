package ru.rshbintech.rsbankws.proxy.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SeqType {

    CASH("ndeal"),
    SECURITIES("tick");

    private final String name;

}
