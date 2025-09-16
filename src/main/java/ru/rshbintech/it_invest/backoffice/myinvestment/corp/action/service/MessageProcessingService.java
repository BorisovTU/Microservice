package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.CorpActionsOptDetailsDao;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.*;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.message.BaseMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CorpActionJobProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.*;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class MessageProcessingService {

    private final MessageRepository messageRepository;
//    private final CorpActionsRepository corpActionsRepository;
    private final SecurityRepository securityRepository;
    private final ClientAccRepository clientAccRepository;
//    private final ResultRepository resultRepository;
    private final LinkRepository linkRepository;
    private final ObjectMapper objectMapper;
    private final CorpActionJobProperties jobProperties;
//    private final CorpActionsOptDetailsRepository corpActionsOptDetailsRepository;
    private final CorpActionsOptDetailsDao corpActionsOptDetailsDao;
    private final ResultRepository resultRepository;
    private final CorpActionsRepository corpActionsRepository;

    @Transactional
    public void run() {
        List<BaseMessage> messages = messageRepository.findByStatusWithLock(0, jobProperties.getConsumerBatchSize());
        log.debug("Found {} new messages", messages.size());
        // Обработка каждого сообщения
        for (BaseMessage message : messages) {
            processMessage(message);
        }
    }

    private void processMessage(BaseMessage message) {
        try {
            CorporateActionNotificationRequestDTO dto =
                    objectMapper.readValue(message.getJson(), CorporateActionNotificationRequestDTO.class);

            CorporateActionNotificationRequest notification =
                    dto.getCorporateActionNotification();


            // 1. Сохранить CorpActions
            saveCorpAction(notification);

            // 2. Сохранить Security
            saveSecurity(notification.getFinInstrmId());

            // 3. Сохранить ClientAcc и Result для каждого владельца
            long caid = Long.parseLong(notification.getCorporateActionIssuerID());
            for (BnfclOwnrDtlsRequest owner : notification.getBnfclOwnrDtls()) {
                ClientAccEntity clientAcc = saveClientAcc(owner);
                ResultEntity result = saveResult(owner, notification);
                saveLink(caid, clientAcc.getId(), result.getClientDiaId(), result.getId());
            }


            // 4. Сохраняем CorpActionsOptDetails
            corpActionsOptDetailsDao.save(caid,notification.getCorpActnOptnDtls());

            // Обновить статус сообщения
            message.setStatus(1); // PARSED_SUCCESS
            log.debug("Saved new message: {}", message);
            message.setCaid(caid);
        } catch (Exception e) {
            log.error("Error processing Message", e);
            message.setStatus(-1); // FLK_ERROR
        }
     //   messageRepository.save(message);
    }

    private void saveCorpAction(
            CorporateActionNotificationRequest notification
    ) {
        String reference = notification.getCorpActnEvtId();
        long caid = Long.parseLong(notification.getCorporateActionIssuerID());
        if (corpActionsRepository.existsCorpActionsEntityByReference(reference)) {
            log.warn("details: Corp action already exists, reference: {}", reference);
            return;
        }
        if (corpActionsRepository.existsCorpActionsEntityByCaid(caid)) {
            log.warn("details: Corp action already exists, caid: {}", caid);
            return;
        }

        CorpActionsEntity entity = dtoToEntity(notification, caid, reference);
        corpActionsRepository.save(entity);
    }

    private static CorpActionsEntity dtoToEntity(CorporateActionNotificationRequest notification, long caid, String reference) {
        CorpActionsEntity entity = new CorpActionsEntity();
        entity.setCaid(caid);
        entity.setCaType(notification.getCorporateActionType());
        entity.setReference(reference);
        entity.setSwiftType(notification.getEvtTp());
        entity.setMaVoCode(notification.getMndtryVlntryEvtTp());
        entity.setAddtlInf(notification.getAddtlInf());
        entity.setLwsInPlcCd(notification.getLwsInPlcCd());
        entity.setSbrdntLwsInPlcCd(notification.getSbrdntLwsInPlcCd());
        entity.setOrgNm(notification.getOrgNm());
        entity.setSfkpgAcct(notification.getSfkpgAcct());
        if (StringUtils.hasText(notification.getRcrdDt())) {
            entity.setDateRegOwners(LocalDate.parse(notification.getRcrdDt()));
        }
        entity.setIsin(notification.getFinInstrmId().getIsin());

        List<CorpActnOptnDtls> corpActnOptnDtls = notification.getCorpActnOptnDtls();
        LocalDate minStartDate = corpActnOptnDtls.stream()
                .map(CorpActnOptnDtls::getActnPrd)
                .filter(Objects::nonNull)
                .map(ActnPrd::getStartDt)
                .filter(StringUtils::hasText)
                .map(LocalDate::parse)
                .min(LocalDate::compareTo)
                .orElse(null);

        LocalDate maxEndDate = corpActnOptnDtls.stream()
                .map(CorpActnOptnDtls::getActnPrd)
                .filter(Objects::nonNull)
                .map(ActnPrd::getEndDt)
                .filter(StringUtils::hasText)
                .map(LocalDate::parse)
                .max(LocalDate::compareTo)
                .orElse(null);

        entity.setMinDateStart(minStartDate);
        entity.setMaxDateEnd(maxEndDate);
        return entity;
    }

    private void saveSecurity(
            FinInstrmId finInstrmId
    ) {
        // Пытаемся найти существующую запись
        Optional<SecurityEntity> existing = securityRepository.findByIsin(finInstrmId.getIsin());
        SecurityEntity entity = existing.orElseGet(SecurityEntity::new);

        entity.setIsin(finInstrmId.getIsin());
        entity.setRegNumber(finInstrmId.getRegNumber());
        entity.setNsdr(finInstrmId.getNsdr());

        securityRepository.save(entity);
    }

    private ClientAccEntity saveClientAcc(
            BnfclOwnrDtlsRequest owner
    ) {
        // Используем accDepo как первичный ключ
        ClientAccEntity entity = clientAccRepository.findByAccDepo(owner.getAcct())
                .orElseGet(ClientAccEntity::new);

        entity.setAccDepo(owner.getAcct());
        entity.setCftid(Long.parseLong(owner.getCftid()));
        entity.setSubAccDepo(owner.getSubAcct());
        entity.setBal(Long.parseLong(owner.getBal()));

        return clientAccRepository.save(entity);
    }

    private ResultEntity saveResult(
            BnfclOwnrDtlsRequest owner,
            CorporateActionNotificationRequest notification
    ) {
        // Используем первый вариант (если есть)
        CorpActnOptnDtls firstOption =
                !notification.getCorpActnOptnDtls().isEmpty()
                        ? notification.getCorpActnOptnDtls().get(0)
                        : null;

        ResultEntity entity = new ResultEntity();
        entity.setClientDiaId(Long.parseLong(owner.getOwnerSecurityID()));
        entity.setSecQtyMess(Float.parseFloat(owner.getBal()));
        entity.setSecQtyClient(0.0f); // По умолчанию

        if (firstOption != null) {
            entity.setDefaultOptions(
                    "1".equals(firstOption.getDfltOptnInd()) ||
                            Boolean.parseBoolean(firstOption.getDfltOptnInd())
            );
        }

        entity.setStatus(0); // NEW
        entity.setCreateDateTime(ZonedDateTime.now());
        resultRepository.save(entity);
        return entity;
    }

    private void saveLink(Long caid, UUID accId, long clientDiaId, UUID resultId) {
        LinkEntity entity = new LinkEntity();
        entity.setCaid(caid);
        entity.setAccId(accId);
        entity.setResultId(resultId);
        linkRepository.save(entity);
    }
}
