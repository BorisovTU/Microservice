package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class InstructionViewService {


    private final ViewCaInstructionRepository instructionRepository;
    private final ObjectMapper objectMapper;

    public Optional<CorporateActionInstruction> getInstructionByNumber(String instrNmb) {
        try {
            Long instrNumber = Long.parseLong(instrNmb);
            Optional<ViewCaInstruction> instruction = instructionRepository.findByInstrNmb(instrNumber);

            return instruction.map(viewInstruction -> {
                try {
                    return objectMapper.readValue(viewInstruction.getPayload(), CorporateActionInstruction.class);
                } catch (Exception e) {
                    log.error("Failed to deserialize instruction payload: {}", viewInstruction.getPayload(), e);
                    return null;
                }
            });
        } catch (NumberFormatException e) {
            log.error("Invalid instruction number format: {}", instrNmb, e);
            return Optional.empty();
        }
    }

    public List<CorporateActionInstruction> getInstructions(String status, Integer limit, String nextid) {
        try {
            Long nextIdLong = nextid != null ? Long.parseLong(nextid) : null;
            PageRequest pageRequest = PageRequest.of(0, limit != null ? limit : 50);

            List<ViewCaInstruction> instructions;
            if (status != null) {
                instructions = instructionRepository.findByStatusWithPagination(status, nextIdLong, pageRequest);
            } else {
                instructions = instructionRepository.findWithPagination(nextIdLong, pageRequest);
            }

            return instructions.stream()
                    .map(viewInstruction -> {
                        try {
                            return objectMapper.readValue(viewInstruction.getPayload(), CorporateActionInstruction.class);
                        } catch (Exception e) {
                            log.error("Failed to deserialize instruction payload: {}", viewInstruction.getPayload(), e);
                            return null;
                        }
                    })
                    .filter(instruction -> instruction != null)
                    .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            log.error("Invalid nextid format: {}", nextid, e);
            return List.of();
        }
    }

    public String getNextId(List<CorporateActionInstruction> instructions, Integer limit) {
        if (instructions.size() < limit) {
            return null;
        }

        CorporateActionInstruction lastInstruction = instructions.get(instructions.size() - 1);
        return lastInstruction.getInstrNmb();
    }
}
