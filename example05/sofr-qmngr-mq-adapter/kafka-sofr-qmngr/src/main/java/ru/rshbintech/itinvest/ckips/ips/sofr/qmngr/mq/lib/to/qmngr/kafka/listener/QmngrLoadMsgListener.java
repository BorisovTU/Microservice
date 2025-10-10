package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.listener;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType.KAFKA;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.MSG_KEY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.MSG_VALUE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.TOPIC;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventParamsBuilderFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventSenderService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr.QmngrLoadMsgService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.converter.ConsumerRecordToQmngrLoadMsgDtoConverter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler.KafkaConsumerRecordHeadersHandler;

/**
 * Потребитель сообщений из Kafka с последующим их сохранением в SOFR QManager.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrLoadMsgListener implements MessageListener<String, String> {

  private final QmngrLoadMsgService loadMsgService;
  private final MessageLoggingService messageLoggingService;
  private final AuditEventSenderService auditEventSenderService;
  private final AuditEventParamsBuilderFactory auditEventParamsBuilderFactory;
  private final KafkaConsumerRecordHeadersHandler kafkaConsumerRecordHeadersHandler;
  private final ConsumerRecordToQmngrLoadMsgDtoConverter consumerRecordToQmngrLoadMsgDtoConverter;

  @Override
  public void onMessage(@NonNull ConsumerRecord<String, String> consumerRecord) {
    final QmngrLoadMsgDto loadMsg;
    try {
      loadMsg = consumerRecordToQmngrLoadMsgDtoConverter.convert(consumerRecord);
    } catch (Exception e) {
      auditKafkaMessageProcessingError(consumerRecord, e);
      return;
    }
    loadMsgService.process(loadMsg, KAFKA);
  }

  private void auditKafkaMessageProcessingError(@NonNull ConsumerRecord<String, String> consumerRecord,
                                                @NonNull Exception exception) {
    final String topic = consumerRecord.topic();
    final String headers = kafkaConsumerRecordHeadersHandler.toJsonString(consumerRecord.headers());
    final String messageKey = consumerRecord.key();
    final String messageValue = consumerRecord.value();
    log.error(
        "Error while processing received message = [ConsumerRecord(topic = {}, key = {})]{} from Kafka. Cause: {}.",
        topic,
        messageKey,
        messageLoggingService.getLogMessageText(headers, messageValue),
        ExceptionUtils.getStackTrace(exception)
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка обработки сообщения из Kafka",
        MQ_LOAD_MSG_PROCESSING_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, topic)
            .withParam(MSG_KEY, messageKey)
            .withHeadersParam(HEADERS, headers)
            .withMessageParam(MSG_VALUE, messageValue)
            .build()
    );
  }

}
