package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCaInstruction;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCaOwnerBalanceRepository;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCaInstructionRepository;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLong;

@Component
@RequiredArgsConstructor
@Slf4j
public class CorporateActionInstructionDao {
    private final ObjectMapper objectMapper;
    private final DataCaOwnerBalanceRepository dataCaOwnerBalanceRepository;
    private final CorporateActionNotificationDao corporateActionNotificationDao;
    private final ViewCaInstructionRepository viewInstructionRepository;
    public String getNotificationPayload(Long ownerId) {
        DataCaOwnerBalance referenceById = dataCaOwnerBalanceRepository.getReferenceById(ownerId);
        if (referenceById == null) {
            log.error("Can't find reference by id {}", ownerId);
            throw new FlkException("Entity not found","No owner balance found for id: " + ownerId);
        }
        return corporateActionNotificationDao.getByCaIdAndCftId(referenceById.getCaid(), referenceById.getCftid());
    }

    @Transactional
    public void saveInstruction(CorporateActionInstruction instruction) throws JsonProcessingException {
        ViewCaInstruction viewInstruction = new ViewCaInstruction();
        String payload = objectMapper.writeValueAsString(instruction);
        viewInstruction.setPayload(payload);
        viewInstruction.setInstrDt(instruction.getInstrDt());
        viewInstruction.setInstrNmb(parseLong(instruction.getInstrNmb(),"InstrNmb is not valid: {}"));
        viewInstructionRepository.save(viewInstruction);
    }
}
