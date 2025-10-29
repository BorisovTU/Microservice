package ru.rshbintech.rsbankws.proxy.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum XmlType {

    SOAP_ENVELOPE("Envelope"),
    METHOD_CALL("MethodCall"),
    PROCESS_DEALS("ProcessDeals_req");

    private final String name;

}
