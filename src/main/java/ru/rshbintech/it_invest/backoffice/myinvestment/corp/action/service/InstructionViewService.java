package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionInstructionDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionViewInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.InstructionMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction.InstructionPaginationStrategy;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction.PaginationRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service.pagination.instruction.PaginationStrategyFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
    private final PaginationStrategyFactory paginationStrategyFactory;

    public Optional<CorporateActionViewInstruction> getInstructionByNumber(String instrNmb) {
        try {
            UUID instrNumber = UUID.fromString(instrNmb);
            Optional<ViewCaInstruction> instruction = instructionRepository.findByInstrNmb(instrNumber);

            return instruction.map(viewInstruction -> {
                try {
                    return objectMapper.readValue(viewInstruction.getPayload(), CorporateActionViewInstruction.class);
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

    public PaginatedInstructionsResult getPaginatedInstructions(String status, Integer limit,
                                                                String cftid, String sort, String nextid) {
        try {
            Long cftidLong = parseLong(cftid, "cftid error format:{}");
            PaginationRequest request = new PaginationRequest(cftidLong, status, limit, sort, nextid);
            InstructionPaginationStrategy strategy = paginationStrategyFactory.getStrategy(request.getInstructionSortType());

            return strategy.getPaginatedInstructions(request, instructionRepository, objectMapper);
        } catch (Exception e) {
            log.error("Error getting paginated instructions: {}", e.getMessage(), e);
            return new PaginatedInstructionsResult(List.of(), null);
        }
    }

    public void postView(CorporateActionInstructionRequest instructionRequest) {
        String ownerSecurityID = instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID();
        if (!StringUtils.hasText(ownerSecurityID)) {
            log.error("OwnerSecutityId must not be empty");
            return;
        }
        try {
            long ownerSecurityIDLong = Long.parseLong(ownerSecurityID);
            CorporateActionNotification corporateActionNotification = corporateActionInstructionAdapter.getCorporateActionNotification(ownerSecurityIDLong);
            CorporateActionViewInstruction corporateActionViewInstruction = instructionMapper.mapToInstructionView(instructionRequest, corporateActionNotification);
            corporateActionInstructionDao.saveInstructionView(corporateActionViewInstruction);

        } catch (NumberFormatException e) {
            log.error("OwnerSecutityId format exception:{}", ownerSecurityID);
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            log.error("Error saving viewInstruction", e);
            throw new RuntimeException(e);
        }
    }

    @Data
    public static class PaginatedInstructionsResult {
        private final List<CorporateActionViewInstruction> instructions;
        private final String nextId;
    }
}
