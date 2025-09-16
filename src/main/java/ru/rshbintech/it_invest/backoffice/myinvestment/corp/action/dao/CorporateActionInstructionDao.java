package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception.FlkException;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCaOwnerBalanceRepository;

@Component
@RequiredArgsConstructor
public class CorporateActionInstructionDao {
    private final DataCaOwnerBalanceRepository dataCaOwnerBalanceRepository;
    private final CorporateActionNotificationDao corporateActionNotificationDao;
    public String getNotificationPayload(Long ownerId) {
        DataCaOwnerBalance referenceById = dataCaOwnerBalanceRepository.getReferenceById(ownerId);
        if (referenceById == null) {
            throw new FlkException("Entity not found","No owner balance found for id: " + ownerId);
        }
        return corporateActionNotificationDao.getByCaIdAndCftId(referenceById.getCaid(), referenceById.getCftid());
    }
}
