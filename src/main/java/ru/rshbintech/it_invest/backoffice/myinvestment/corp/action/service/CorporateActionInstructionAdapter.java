package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorporateActionInstructionDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.UUID;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLocalDate;
import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseOffsetDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class CorporateActionInstructionAdapter {
    private final ObjectMapper objectMapper;
    private final CorporateActionInstructionDao corporateActionInstructionDao;
    private final KafkaTemplate<String, CorporateActionInstructionRequest> internalInstructionBalanceKafkaTemplate;
    private final CustomKafkaProperties customKafkaProperties;

    public void processInstruction(@Valid CorporateActionInstructionRequest instructionRequest) throws JsonProcessingException {
        validate(instructionRequest);
        String topic = customKafkaProperties.getInternalInstructionBalance().getTopic();
        internalInstructionBalanceKafkaTemplate.send(topic, instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID(), instructionRequest);
    }

    private void validate(CorporateActionInstructionRequest instructionRequest) {
        if (corporateActionInstructionDao.existsInstructionViewByInstrNmb(UUID.fromString(instructionRequest.getInstrNmb()))) {
            throw new FlkException("DUBLICATE","Инструкция с данным instrNmb уже существует");
        }
        long ownerSecurityID = Long.parseLong(instructionRequest.getBnfclOwnrDtls().getOwnerSecurityID());
        CorporateActionNotification corporateActionNotification = getCorporateActionNotification(ownerSecurityID);
        String optnNb = instructionRequest.getCorpActnOptnDtls().getOptnNb();
        validateOptDate(corporateActionNotification, optnNb);
        validateCaDateTime(corporateActionNotification);
    }

    private static void validateCaDateTime(CorporateActionNotification corporateActionNotification) {
        CorporateActionNotification.RspnPrd actnPrd = corporateActionNotification.getActnPrd();
        if (actnPrd != null) {
            LocalDate caStart = parseLocalDate(actnPrd.getStartDt(), "invalid startDt format: {}");
            if (caStart != null && caStart.isAfter(LocalDate.now())) {
                log.error("Дата начала действия опции еще не наступила");
                throw new FlkException("DATE_NOT_STARTED", "Дата начала действия КД еще не наступила");
            }
            OffsetDateTime caEnd = parseOffsetDateTime(actnPrd.getRspnDdln(),"Время действия предложения уже завершилось или было отменено");
            if (caEnd != null && caEnd.isBefore(OffsetDateTime.now())) {
                log.error("Время действия предложения уже завершилось или было отменено");
                throw new FlkException("DATE_ALREADY_END", "Время действия КД уже завершилось или было отменено");
            }
        }
    }

    private static void validateOptDate(CorporateActionNotification corporateActionNotification, String optnNb) {
        if (corporateActionNotification.getCorpActnOptnDtls() != null) {
            CorporateActionNotification.CorpActnOptnDtls corpActnOptnDtls = corporateActionNotification.getCorpActnOptnDtls().stream()
                    .filter(dtls -> optnNb.equals(dtls.getOptnNb())).findFirst().orElseThrow(() -> new FlkException("corpActnOptnDtls_NOT_AVAILABLE", "Опция не найдена " + optnNb));
            if (corpActnOptnDtls.getActnPrd() != null) {
                LocalDate localStartDate = parseLocalDate(corpActnOptnDtls.getActnPrd().getStartDt(), "invalid startDt format: {}");
                if (localStartDate != null && localStartDate.isAfter(LocalDate.now())) {
                    log.error("Дата начала действия опции еще не наступила");
                    throw new FlkException("DATE_NOT_STARTED", "Дата начала действия опции еще не наступила");
                }
                if (corpActnOptnDtls.getActnPrd().getEndDt() != null) {
                    LocalDate localEndDate = parseLocalDate(corpActnOptnDtls.getActnPrd().getEndDt(), "invalid startDt format: {}");
                    if (localEndDate != null && localEndDate.isBefore(LocalDate.now())) {
                        log.error("Время действия предложения уже завершилось или было отменено");
                        throw new FlkException("DATE_ALREADY_END", "Время действия предложения уже завершилось или было отменено");
                    }
                }
            }
        }
    }

    public CorporateActionNotification getCorporateActionNotification(Long ownerSecurityID) {
        String notificationPayload = corporateActionInstructionDao.getNotificationPayload(ownerSecurityID);
        CorporateActionNotification corporateActionNotification = null;
        try {
            corporateActionNotification = objectMapper.readValue(notificationPayload, CorporateActionNotificationDto.class).getCorporateActionNotification();
        } catch (JsonProcessingException e) {
            log.error("Запись в БД не может быть распарсена");
            throw new RuntimeException("Запись в БД не может быть распарсена");
        }
        return corporateActionNotification;
    }
}
