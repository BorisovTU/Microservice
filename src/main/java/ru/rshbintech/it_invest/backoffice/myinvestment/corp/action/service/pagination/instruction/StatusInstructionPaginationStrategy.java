package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionViewInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionViewService;

import java.util.List;

@Component
@RequiredArgsConstructor
public class StatusInstructionPaginationStrategy implements InstructionPaginationStrategy {

    @Override
    public InstructionViewService.PaginatedInstructionsResult getPaginatedInstructions(
            PaginationRequest request,
            ViewCaInstructionRepository repository,
            ObjectMapper objectMapper) {

        List<ViewCaInstruction> viewInstructions;
        if (request.getStatus() != null) {
            viewInstructions = repository.findByStatusWithStatusPagination(
                    request.getCftid(),
                    request.getStatus(),
                    request.getNextStatus(),
                    request.getNextInstrNmb(),
                    request.getLimit() + 1
            );
        } else {
            viewInstructions = repository.findWithStatusPagination(
                    request.getCftid(),
                    request.getNextStatus(),
                    request.getNextInstrNmb(),
                    request.getLimit() + 1
            );
        }

        List<CorporateActionViewInstruction> instructions = convertToViewInstructions(viewInstructions, objectMapper);
        String nextId = generateNextId(viewInstructions, request.getLimit());

        if (instructions.size() > request.getLimit()) {
            instructions = instructions.subList(0, request.getLimit());
        }

        return new InstructionViewService.PaginatedInstructionsResult(instructions, nextId);
    }

    private String generateNextId(List<ViewCaInstruction> instructions, int limit) {
        if (instructions.size() <= limit) {
            return null;
        }

        ViewCaInstruction lastInstruction = instructions.get(limit - 1);
        return lastInstruction.getStatus() + "," + lastInstruction.getInstrNmb();
    }
}
