package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.impl;

import jakarta.persistence.EntityNotFoundException;
import jakarta.validation.ValidationException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.*;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.UUID;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.validateLong;

@Component
@RequiredArgsConstructor
public class CorporateActionDaoImpl implements ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionDao {

    private final ClientAccRepository clientAccRepository;
    private final CorpActionsRepository corpActionsRepository;
    private final SecurityRepository securityRepository;
    private final ResultRepository resultRepository;
    private final LinkRepository linkRepository;

    public ClientAccEntity findClientAcc(String accDepo, Long cftid) {
        return clientAccRepository.findByAccDepoAndCftid(accDepo, cftid)
                .orElseThrow(() -> new EntityNotFoundException("ClientAcc not found " + accDepo + " : " + cftid));
    }

    public CorpActionsEntity findCorpActionByReference(String reference) {
        return corpActionsRepository.findByReference(reference)
                .orElseThrow(() -> new EntityNotFoundException("CorpActions not found " + reference));
    }

    private LinkEntity findLinkByCaIdAndAccId(Long caid, UUID id) {
        return linkRepository.findFirstByCaidAndAccId(caid, id)
                .orElseThrow(() -> new EntityNotFoundException("Отсутствует связь между счетом " + id + " и КД - " + caid));
    }

    public SecurityEntity findSecurityByIsin(String isin) {
        return securityRepository.findByIsin(isin)
                .orElseThrow(() -> new EntityNotFoundException("Security not found " + isin));
    }

    @Transactional
    @Override
    public ResultEntity saveInstructionResult(
            CorporateActionInstructionDTO.CorporateActionInstruction instruction,
            CorporateActionInstructionDTO.ClientId client,
            CorporateActionInstructionDTO.CorpActnOptnDtls option) {

        // Проверяем существование связанных сущностей
        LinkEntity link = checkAndGetLink(instruction, client);

        ResultEntity result = buildResultResponse(instruction, option, link);

        return resultRepository.save(result);
    }

    private ResultEntity buildResultResponse(CorporateActionInstructionDTO.CorporateActionInstruction instruction, CorporateActionInstructionDTO.CorpActnOptnDtls option, LinkEntity link) {
        // Создаем и сохраняем результат
        Long clientDiaId = resultRepository.getClientDiaIdById(link.getResultId());
        ResultEntity result = new ResultEntity();
        //   result.setId(UUID.randomUUID());
        result.setRedemptionPrice(option.getPricVal());
        result.setRedemptionCurrency(option.getPricValCcy());
        result.setOptNumber(Integer.parseInt(option.getOptnNb()));
        result.setOptType(option.getOptnTp());
        result.setSecQtyClient(option.getBal().floatValue());
        result.setRequestId(link.getResultId());
        result.setClientDiaId(clientDiaId);
        result.setStatus(2);

        try {
            ZonedDateTime instrDt = ZonedDateTime.ofInstant(Instant.parse(instruction.getInstrDt()), ZoneId.systemDefault());
            result.setInstrDt(instrDt);
        } catch (DateTimeParseException e) {
            throw new ValidationException("instrDt is not a valid date");
        }
        try {
            result.setInstrNmb(Integer.parseInt(instruction.getInstrNmb()));
        } catch (NumberFormatException e) {
            throw new ValidationException("instrNmb is not a valid number");
        }

        result.setCreateDateTime(ZonedDateTime.now());
        return result;
    }

    private LinkEntity checkAndGetLink(CorporateActionInstructionDTO.CorporateActionInstruction instruction, CorporateActionInstructionDTO.ClientId client) {
        if (!instruction.getAcct().startsWith("Д-Т-") && !instruction.getAcct().startsWith("Д-T-") ) {
            throw new ValidationException("Поле '" + instruction.getAcct() + "' acct не соответствует маске Д-Т-\\d{6}");
        }
        validateLong(instruction.getAcct().replace("Д-Т-","").replace("Д-T-",""),"Поле acct не соответствует маске Д-Т-\\d{6}");
        ClientAccEntity clientAcc = findClientAcc(instruction.getAcct(), Long.parseLong(client.getObjectId()));
        CorpActionsEntity corpActionByReference = findCorpActionByReference(instruction.getCorpActnEvtId());
        if (instruction.getIsin() != null) {
            findSecurityByIsin(instruction.getIsin());
        }
        return findLinkByCaIdAndAccId(corpActionByReference.getCaid(),clientAcc.getId());
    }
}