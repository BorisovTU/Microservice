package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.InstructionSortType;

@Component
@RequiredArgsConstructor
public class PaginationStrategyFactory {

    private final InstrDtInstructionPaginationStrategy instrDtStrategy;
    private final InstrNmbInstructionPaginationStrategy instrNmbStrategy;
    private final StatusInstructionPaginationStrategy statusStrategy;

    public InstructionPaginationStrategy getStrategy(InstructionSortType instructionSortType) {
        switch (instructionSortType) {
            case INSTR_DT:
                return instrDtStrategy;
            case INSTR_NMB:
                return instrNmbStrategy;
            case STATUS:
                return statusStrategy;
            default:
                return instrDtStrategy;
        }
    }
}
