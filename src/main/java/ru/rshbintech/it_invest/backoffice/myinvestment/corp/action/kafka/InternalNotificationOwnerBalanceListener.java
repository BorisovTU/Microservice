package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCaOwnerBalance;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.OwnerBalanceMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCaOwnerBalanceRepository;

import java.util.List;

@EnableKafka
@Component
@Slf4j
@RequiredArgsConstructor
public class InternalNotificationOwnerBalanceListener {

    private final DataCaOwnerBalanceRepository repository;
    private final OwnerBalanceMapper ownerBalanceMapper;

    @KafkaListener(
            groupId = "${kafka.internal-notification-owner-balance.consumer.group-id}",
            topics = "${kafka.internal-notification-owner-balance.topic}",
            containerFactory = BeanConstants.INTERNAL_NOTIFICATION_OWNER_BALANCE
    )
    @Transactional
    public void processInternalNotificationOwnerBalance(CorporateActionNotificationDto notification) {
        try {
            log.info("processInternalNotificationOwnerBalance: {}", notification);
            List<DataCaOwnerBalance> ownerEntities = ownerBalanceMapper.mapToOwnerBalanceEntities(notification);

            if (!ownerEntities.isEmpty()) {
                repository.saveAll(ownerEntities);
                log.info("Saved {} internal owner balances {}",
                        ownerEntities.size(), notification.getCorporateActionNotification().getCorporateActionIssuerID());
            } else {
                log.info("No internal owner balances found");
            }
        } catch (Exception e) {
            log.error("Failed to process view notification for corporate action: {}",
                    notification.getCorporateActionNotification().getCorporateActionIssuerID(), e);
        }
    }
}
