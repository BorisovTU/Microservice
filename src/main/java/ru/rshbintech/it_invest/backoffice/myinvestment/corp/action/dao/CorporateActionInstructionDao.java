package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionViewInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReq;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCAInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCAInstructionRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCaOwnerBalanceRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLong;

@Component
@RequiredArgsConstructor
@Slf4j
public class CorporateActionInstructionDao {
    private final ObjectMapper objectMapper;
    private final DataCaOwnerBalanceRepository dataCaOwnerBalanceRepository;
    private final CorporateActionNotificationDao corporateActionNotificationDao;
    private final ViewCaInstructionRepository viewInstructionRepository;
    private final DataCAInstructionRepository dataCAInstructionRepository;

    public String getNotificationPayload(Long ownerId) {
        DataCaOwnerBalance referenceById = dataCaOwnerBalanceRepository.getReferenceById(ownerId);
        if (referenceById == null) {
            log.error("Can't find reference by id {}", ownerId);
            throw new FlkException("Entity not found", "No owner balance found for id: " + ownerId);
        }
        return corporateActionNotificationDao.getByCaIdAndCftId(referenceById.getCaid(), referenceById.getCftid());
    }

    @Transactional
    public void saveInstructionView(CorporateActionViewInstruction instruction) throws JsonProcessingException {
        ViewCaInstruction viewInstruction = new ViewCaInstruction();
        String payload = objectMapper.writeValueAsString(instruction);
        viewInstruction.setPayload(payload);
        viewInstruction.setInstrDt(instruction.getInstrDt());
        viewInstruction.setStatus(instruction.getStatus());
        if (instruction.getBnfclOwnrDtls() != null) {
            viewInstruction.setCftid(parseLong(instruction.getBnfclOwnrDtls().getCftid(), "cftid is not valid: {}"));
        }
        viewInstruction.setInstrNmb(UUID.fromString(instruction.getInstrNmb()));
        viewInstructionRepository.save(viewInstruction);
    }

    @Transactional
    public void saveInstruction(SendCorpActionsAssignmentReq corporateActionInstruction) throws JsonProcessingException {
        DataCAInstruction dataCAInstruction = new DataCAInstruction();
        String payload = objectMapper.writeValueAsString(corporateActionInstruction);
        dataCAInstruction.setPayload(payload);
        dataCAInstruction.setCreateDateTime(OffsetDateTime.now());
        if (corporateActionInstruction != null
                && corporateActionInstruction.getSendCorpActionsAssignmentReq() != null
                && corporateActionInstruction.getSendCorpActionsAssignmentReq().getCorporateActionInstruction() != null) {
            String ownerSecurityID = corporateActionInstruction.getSendCorpActionsAssignmentReq().getCorporateActionInstruction().getOwnerSecurityID();
            dataCAInstruction.setOwnerSecurityId(parseLong(ownerSecurityID, "ownerSecurityId is not valid: {}"));
        }
        dataCAInstructionRepository.save(dataCAInstruction);
    }

    @Transactional
    public BigDecimal getOwnerSecurityBalance(Long ownerSecurityID) {
        DataCaOwnerBalance referenceById = dataCaOwnerBalanceRepository.getReferenceById(ownerSecurityID);
        if (referenceById == null) {
            return null;
        }
        return referenceById.getBal();
    }
}
