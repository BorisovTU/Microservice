package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction;

import com.fasterxml.jackson.databind.ObjectMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionViewInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.InstructionViewService;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface InstructionPaginationStrategy {
    InstructionViewService.PaginatedInstructionsResult getPaginatedInstructions(
            PaginationRequest request,
            ViewCaInstructionRepository repository,
            ObjectMapper objectMapper);

    default List<CorporateActionViewInstruction> convertToViewInstructions(
            List<ViewCaInstruction> viewInstructions, ObjectMapper objectMapper) {
        return viewInstructions.stream()
                .map(viewInstruction -> {
                    try {
                        return objectMapper.readValue(viewInstruction.getPayload(), CorporateActionViewInstruction.class);
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
