package ru.rshbintech.it_invest.backoffice.myinvestment.finmarkets.derivatives.enrichment_deals.models.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Getter
public enum ExecTypes {
    NEW("0", "NEW", "Размещение заявки"),
    DONE_FOR_DAY("3", "DONE FOR DAY", "Завершение сессии"),
    CANCELED("4", "CANCELED", "Удаление/снятие заявки"),
    REPLACED("5", "REPLACED", "Замена заявки"),
    PENDING_CANCEL("6", "PENDING CANCEL", "Ожидается снятие"),
    REJECTED("8", "REJECTED", "Отклонение запроса"),
    EXPIRED("C", "EXPIRED", "Просрочена"),
    PENDING_REPLACE("E", "PENDING REPLACE", "Ожидается замена"),
    TRADE("F", "TRADE", "Сделка");

    private final String code;
    private final String name;
    private final String description;

    private static final Map<String, ExecTypes> mapByCode = new HashMap<>();

    static {
        for (ExecTypes execType : values()) {
            mapByCode.put(execType.getCode(), execType);
        }
    }

    public static String getNameByCode(String code) {
        ExecTypes execType = mapByCode.get(code);
        return execType != null ? execType.getName() : null;
    }

    @Override
    public String toString() {
        return name;
    }
}
