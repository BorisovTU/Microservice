package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction;

import lombok.Data;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.InstructionSortType;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
public class PaginationRequest {
    private final Long cftid;
    private final String status;
    private final Integer limit;
    private final InstructionSortType instructionSortType;
    private OffsetDateTime nextInstrDt;
    private UUID nextInstrNmb;
    private String nextStatus;

    public PaginationRequest(Long cftid, String status, Integer limit, String sort, String nextid) {
        this.cftid = cftid;
        this.status = status;
        this.limit = limit != null ? limit : 50;
        this.instructionSortType = InstructionSortType.fromString(sort);

        // Parse nextid based on sort type
        if (nextid != null) {
            parseNextId(nextid);
        }
    }

    private void parseNextId(String nextid) {
        switch (this.instructionSortType) {
            case INSTR_DT:
                String[] dtParts = nextid.split(",");
                if (dtParts.length == 2) {
                    this.nextInstrDt = OffsetDateTime.parse(dtParts[0]);
                    this.nextInstrNmb = UUID.fromString(dtParts[1]);
                }
                break;
            case INSTR_NMB:
                this.nextInstrNmb = UUID.fromString(nextid);
                break;
            case STATUS:
                String[] statusParts = nextid.split(",");
                if (statusParts.length == 2) {
                    this.nextStatus = statusParts[0];
                    this.nextInstrNmb = UUID.fromString(statusParts[1]);
                }
                break;
        }
    }
}