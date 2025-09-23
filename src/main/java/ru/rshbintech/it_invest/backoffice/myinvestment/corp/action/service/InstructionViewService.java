package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionInstructionDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.InstructionMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLong;

@Service
@Slf4j
@RequiredArgsConstructor
public class InstructionViewService {

    private final CorporateActionInstructionAdapter corporateActionInstructionAdapter;
    private final InstructionMapper instructionMapper;
    private final CorporateActionInstructionDao corporateActionInstructionDao;
    private final ViewCaInstructionRepository instructionRepository;
    private final ObjectMapper objectMapper;

    public Optional<CorporateActionInstruction> getInstructionByNumber(String instrNmb) {
        try {
            UUID instrNumber = UUID.fromString(instrNmb);
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

    public List<CorporateActionInstruction> getInstructions(String status,
                                                            Integer limit,
                                                            String cftid,
                                                            String nextid) {
        try {
            UUID nextIdLong = nextid != null ? UUID.fromString(nextid) : null;
            PageRequest pageRequest = PageRequest.of(0, limit != null ? limit : 50);

            List<ViewCaInstruction> instructions;
            if (status != null) {
                instructions = instructionRepository.findByStatusWithPagination(parseLong(cftid,"cftid error format:{}"), status, nextIdLong, pageRequest);
            } else {
                instructions = instructionRepository.findWithPagination(parseLong(cftid,"cftid error format:{}"),nextIdLong, pageRequest);
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

    public void postView(CorporateActionInstructionRequest instructionRequest) {
        String ownerSecurityID = instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID();
        CorporateActionNotification corporateActionNotification = corporateActionInstructionAdapter.getCorporateActionNotification(Long.parseLong(ownerSecurityID));
        CorporateActionInstruction corporateActionInstruction = instructionMapper.map(instructionRequest, corporateActionNotification);
        try {
            corporateActionInstructionDao.saveInstructionView(corporateActionInstruction);
        } catch (JsonProcessingException e) {
            log.error("Error saving viewInstruction", e);
        }
    }
}
