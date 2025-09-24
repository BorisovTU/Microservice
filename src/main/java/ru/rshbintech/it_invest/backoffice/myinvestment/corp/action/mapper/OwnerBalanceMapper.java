package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class OwnerBalanceMapper {

    public List<DataCaOwnerBalance> mapToOwnerBalanceEntities(CorporateActionNotificationDto notificationDto) {
        List<DataCaOwnerBalance> entities = new ArrayList<>();

        if (notificationDto != null && notificationDto.getCorporateActionNotification() != null) {
            Long caid = Long.parseLong(notificationDto.getCorporateActionNotification().getCorporateActionIssuerID());

            notificationDto.getCorporateActionNotification().getBnfclOwnrDtls().forEach(owner -> {
                DataCaOwnerBalance entity = new DataCaOwnerBalance();
                entity.setOwnerSecurityId(Long.parseLong(owner.getOwnerSecurityID()));
                entity.setBal(owner.getBal());
                entity.setCreateDateTime(OffsetDateTime.now());
                entity.setCaid(caid);
                entity.setCftid(Long.parseLong(owner.getCftid()));

                entities.add(entity);
            });
        }

        return entities;
    }
}