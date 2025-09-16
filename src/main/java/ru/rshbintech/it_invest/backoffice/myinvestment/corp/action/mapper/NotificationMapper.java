package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseLocalDate;
import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.util.ParseUtil.parseOffsetDateTime;

@Component
@Slf4j
@RequiredArgsConstructor
public class NotificationMapper {

    private final ObjectMapper objectMapper;

    public List<ViewCANotification> mapToViewEntities(CorporateActionNotificationDto notification) {
        CorporateActionNotification corporateActionNotification = notification.getCorporateActionNotification();

        Map<String, List<CorporateActionNotification.BnfclOwnrDtls>> ownersByCftId = corporateActionNotification.getBnfclOwnrDtls()
                .stream()
                .collect(Collectors.groupingBy(CorporateActionNotification.BnfclOwnrDtls::getCftid));

        List<ViewCANotification> viewEntities = new ArrayList<>();

        for (Map.Entry<String, List<CorporateActionNotification.BnfclOwnrDtls>> entry : ownersByCftId.entrySet()) {
            String cftId = entry.getKey();
            List<CorporateActionNotification.BnfclOwnrDtls> ownersForCftId = entry.getValue();

            try {
                CorporateActionNotificationDto notificationForCftId = cloneNotification(notification);
                notificationForCftId.getCorporateActionNotification().setBnfclOwnrDtls(ownersForCftId);

                ViewCANotification viewEntity = new ViewCANotification();
//                viewEntity.setId(UUID.randomUUID());
                viewEntity.setCaid(Long.parseLong(corporateActionNotification.getCorporateActionIssuerID()));
                viewEntity.setCreateDateTime(OffsetDateTime.now());
                viewEntity.setCftid(Long.parseLong(cftId));
                if (corporateActionNotification.getActnPrd() != null) {
                    viewEntity.setStartDt(parseLocalDate(corporateActionNotification.getActnPrd().getStartDt(),"Invalid StartDt format: {}"));
                    viewEntity.setRspnddln(parseOffsetDateTime(corporateActionNotification.getActnPrd().getRspnDdln(),"Invalid Rspnddln format: {}"));
                }
                String payload = objectMapper.writeValueAsString(notificationForCftId);
                log.info("Notification Payload: {}", payload);
                viewEntity.setPayload(payload);

                viewEntities.add(viewEntity);
            } catch (Exception e) {
                log.error("Failed to create view entity for CFTID: {}", cftId, e);
            }
        }

        return viewEntities;
    }

    private CorporateActionNotificationDto cloneNotification(CorporateActionNotificationDto original) {
        CorporateActionNotificationDto clone = new CorporateActionNotificationDto();
        clone.setCorporateActionNotification(new CorporateActionNotification());
        clone.getCorporateActionNotification().setCorporateActionIssuerID(original.getCorporateActionNotification().getCorporateActionIssuerID());
        clone.getCorporateActionNotification().setCorporateActionType(original.getCorporateActionNotification().getCorporateActionType());
        clone.getCorporateActionNotification().setCorpActnEvtId(original.getCorporateActionNotification().getCorpActnEvtId());
        clone.getCorporateActionNotification().setEvtTp(original.getCorporateActionNotification().getEvtTp());
        clone.getCorporateActionNotification().setMndtryVlntryEvtTp(original.getCorporateActionNotification().getMndtryVlntryEvtTp());
        clone.getCorporateActionNotification().setRcrdDt(original.getCorporateActionNotification().getRcrdDt());
        clone.getCorporateActionNotification().setOrgNm(original.getCorporateActionNotification().getOrgNm());
        clone.getCorporateActionNotification().setSfkpgAcct(original.getCorporateActionNotification().getSfkpgAcct());
        clone.getCorporateActionNotification().setFinInstrmId(original.getCorporateActionNotification().getFinInstrmId());
        clone.getCorporateActionNotification().setActnPrd(original.getCorporateActionNotification().getActnPrd());
        clone.getCorporateActionNotification().setAddtlInf(original.getCorporateActionNotification().getAddtlInf());
        clone.getCorporateActionNotification().setLwsInPlcCd(original.getCorporateActionNotification().getLwsInPlcCd());
        clone.getCorporateActionNotification().setSbrdntLwsInPlcCd(original.getCorporateActionNotification().getSbrdntLwsInPlcCd());
        clone.getCorporateActionNotification().setCorpActnOptnDtls(new ArrayList<>(original.getCorporateActionNotification().getCorpActnOptnDtls()));
        return clone;
    }
}
