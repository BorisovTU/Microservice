package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionInstructionDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReq;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.InstructionMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InstructionProcessorService {
    private final KafkaTemplate<String, SendCorpActionsAssignmentReq> instructionToDiasoftKafkaTemplate;
    private final CustomKafkaProperties customKafkaProperties;
    private final CorporateActionInstructionAdapter corporateActionInstructionAdapter;
    private final InstructionMapper instructionMapper;
    private final CorporateActionInstructionDao corporateActionInstructionDao;

    public void processInstruction(CorporateActionInstructionRequest instructionRequest) {
        if (instructionRequest.getInstrNmb() == null) {
            log.error("No instrument number");
            return;
        }
        UUID instrNmber = UUID.fromString(instructionRequest.getInstrNmb());
        if (corporateActionInstructionDao.existsInstructionByInstrNmb(instrNmber)) {
            log.error("Duplicate instrument number");
            return;
        }
        String ownerSecurityID = instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID();
        CorporateActionNotification corporateActionNotification = corporateActionInstructionAdapter.getCorporateActionNotification(Long.parseLong(ownerSecurityID));
        SendCorpActionsAssignmentReq corporateActionInstruction = instructionMapper.mapToSendCorpActionsAssignmentReq(instructionRequest, corporateActionNotification);
        instructionToDiasoftKafkaTemplate.send(customKafkaProperties.getInstructionToDiasoft().getTopic(), corporateActionInstruction);
        try {
            corporateActionInstructionDao.saveInstruction(corporateActionInstruction);
        } catch (JsonProcessingException e) {
            log.error("Error saving instruction", e);
        }
    }

    public BigDecimal getInternalInstructionLimit(Long ownerSecurityID) {
        return corporateActionInstructionDao.getOwnerSecurityBalance(ownerSecurityID);
    }
}
