package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OwnerBalanceMapper {

    public List<DataCaOwnerBalance> mapToOwnerBalanceEntities(CorporateActionNotificationDto notificationDto) {
        List<DataCaOwnerBalance> entities = new ArrayList<>();

        if (notificationDto != null && notificationDto.getCorporateActionNotification() != null) {
            Long caid = Long.parseLong(notificationDto.getCorporateActionNotification().getCorporateActionIssuerID());

            notificationDto.getCorporateActionNotification().getBnfclOwnrDtls().forEach(owner -> {
                DataCaOwnerBalance entity = new DataCaOwnerBalance();
                try {
                    entity.setOwnerSecurityId(Long.parseLong(owner.getOwnerSecurityID()));
                } catch (NumberFormatException e) {
                    log.error("Error while parsing ownerSecutityId. Number format exception for: {}",
                            owner.getOwnerSecurityID());
                    return;
                }
                entity.setBal(owner.getBal());
                entity.setCreateDateTime(OffsetDateTime.now());
                entity.setCaid(caid);
                try {
                    entity.setCftid(Long.parseLong(owner.getCftid()));
                } catch (NumberFormatException e) {
                    log.info("Error while parsing cftId. Number format exception for: {}",
                            owner);
                    return;
                }
                entities.add(entity);
            });
        }

        return entities;
    }
}