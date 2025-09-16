package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dao.MessageDao;

@EnableKafka
@Component
@RequiredArgsConstructor
public class CorporateActionKafkaListener {

    private final MessageDao messageDao;

    @KafkaListener(
            topics = "${kafka.corp-action.notification-from-diasoft-topic}",
            containerFactory = "kafkaListenerContainerFactory"

    )
    public void get(final ConsumerRecord<String, String> record) {
        messageDao.saveIncomeDiasoftMessage(record.value());
    }
}
