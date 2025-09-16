package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.entity.DataCANotification;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.repository.DataCANotificationRepository;

import java.time.OffsetDateTime;

@EnableKafka
@Component
@Slf4j
@RequiredArgsConstructor
public class NotificationFromDiasoftListener {

    private final ObjectMapper objectMapper;
    private final DataCANotificationRepository repository;
    private final CustomKafkaProperties kafkaProperties;
    private final KafkaTemplate<String, CorporateActionNotificationDto> internalNotificationKafkaTemplate;

    @KafkaListener(
            groupId = "${kafka.notification-from-diasoft.consumer.group-id}",
            topics = "${kafka.notification-from-diasoft.topic}",
            containerFactory = BeanConstants.NOTIFICATION_FROM_DIASOFT_CONSUMER_FACTORY
    )
    @Transactional
    public void consume(String message) {
        try {
            CorporateActionNotificationDto notification = objectMapper.readValue(message, CorporateActionNotificationDto.class);

            DataCANotification entity = new DataCANotification();
//            entity.setId(UUID.randomUUID());
            entity.setCaid(Long.parseLong(notification.getCorporateActionNotification().getCorporateActionIssuerID()));
            entity.setCreateDateTime(OffsetDateTime.now());
            entity.setPayload(message);
            repository.save(entity);
            String topic = kafkaProperties.getInternalNotification().getTopic();
            internalNotificationKafkaTemplate.send(topic, notification);

            log.info("Processed message from topic: {}, corporateActionIssuerID: {}", topic, notification.getCorporateActionNotification().getCorporateActionIssuerID());
        } catch (Exception e) {
            log.error("Failed to parse message: {}", message, e);
//            stringKafkaTemplate.send("dlq-topic", message);
        }
    }
}
