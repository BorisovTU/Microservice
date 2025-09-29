package ru.rshbintech.it_invest.backoffice.myinvestment.fx_enrichment_service.enums;

import java.util.Arrays;

public enum OrderStatus {

    NEW("0"),
    PARTIALLY_FILLED("1"),
    FILLED("2"),
    CANCELED("4"),
    PENDING_CANCEL("6"),
    REJECTED("8"),
    PENDING_REPLACE("E"),
    TRADE("F");

    private final String code;

    OrderStatus(String code) { this.code = code; }

    public static OrderStatus fromCode(String code) {
        return Arrays.stream(values())
                .filter(s -> s.code.equals(code))
                .findFirst()
                .orElse(null);
    }

}
