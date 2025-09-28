package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

public enum InstructionSortType {
    INSTR_DT("InstrDt"),
    INSTR_NMB("InstrNmb"),
    STATUS("Status");

    private final String value;

    InstructionSortType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static InstructionSortType fromString(String value) {
        for (InstructionSortType type : values()) {
            if (type.getValue().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return INSTR_DT; // default
    }
}