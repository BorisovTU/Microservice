package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.ViewCANotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.mapper.NotificationMapper;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.ViewCANotificationRepository;

import java.util.List;

@EnableKafka
@Component
@Slf4j
@RequiredArgsConstructor
public class InternalNotificationListener {

    private final ViewCANotificationRepository repository;
    private final NotificationMapper mappingService;

    @KafkaListener(
            groupId = "${kafka.internal-notification.consumer.group-id}",
            topics = "${kafka.internal-notification.topic}",
            containerFactory = BeanConstants.INTERNAL_NOTIFICATION_FACTORY
    )
    @Transactional
    public void processViewNotification(CorporateActionNotificationDto notification) {
        try {
            List<ViewCANotification> viewEntities = mappingService.mapToViewEntities(notification);

            if (!viewEntities.isEmpty()) {
                repository.saveAll(viewEntities);
                log.info("Saved {} view entities for corporate action {}",
                        viewEntities.size(), notification.getCorporateActionNotification().getCorporateActionIssuerID());
            }
        } catch (Exception e) {
            log.error("Failed to process view notification for corporate action: {}",
                    notification.getCorporateActionNotification().getCorporateActionIssuerID(), e);
        }
    }
}
