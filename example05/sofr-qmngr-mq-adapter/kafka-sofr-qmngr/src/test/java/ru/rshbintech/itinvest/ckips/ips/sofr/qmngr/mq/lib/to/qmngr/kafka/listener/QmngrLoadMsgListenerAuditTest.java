package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.listener;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_HEADERS_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType.KAFKA;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.MSG_KEY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums.KafkaAuditEventParam.MSG_VALUE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_HEADERS_WITH_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_STRING_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestConstants.TEST_VAL_TOPIC_NAME_1;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.lang.NonNull;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventParamsBuilderFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventSenderService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr.QmngrLoadMsgService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.converter.ConsumerRecordToQmngrLoadMsgDtoConverter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.handler.KafkaConsumerRecordHeadersHandler;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.utils.TestUtils;

@SuppressWarnings("unchecked")
@ExtendWith(MockitoExtension.class)
class QmngrLoadMsgListenerAuditTest {

  private static final int TRIM_TO_LENGTH = 100;

  private final QmngrLoadMsgService loadMsgService = Mockito.mock(QmngrLoadMsgService.class);
  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);
  private final ConsumerRecordToQmngrLoadMsgDtoConverter consumerRecordToQmngrLoadMsgDtoConverter =
      Mockito.mock(ConsumerRecordToQmngrLoadMsgDtoConverter.class);
  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final MonitoringParamHeadersAuditProperties monitoringParamHeadersAuditProperties =
      Mockito.mock(MonitoringParamHeadersAuditProperties.class);
  private final MonitoringParamMessageAuditProperties monitoringParamMessageAuditProperties =
      Mockito.mock(MonitoringParamMessageAuditProperties.class);

  private final QmngrLoadMsgListener loadMsgListener;

  @Captor
  private ArgumentCaptor<Supplier<Map<String, String>>> auditParamsCaptor;
  @Captor
  private ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  private ArgumentCaptor<AuditEvent> auditEventCaptor;

  QmngrLoadMsgListenerAuditTest() {
    loadMsgListener = new QmngrLoadMsgListener(
        loadMsgService,
        messageLoggingService,
        auditEventSenderService,
        new AuditEventParamsBuilderFactory(
            monitoringParamHeadersAuditProperties,
            monitoringParamMessageAuditProperties
        ),
        new KafkaConsumerRecordHeadersHandler(new ObjectMapper()),
        consumerRecordToQmngrLoadMsgDtoConverter
    );
  }

  @Test
  void testConsumerRecordProcessingErrorWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);
    final Map<String, String> auditParams = makeAuditTest();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME_1, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_KEY.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertEquals(TEST_VAL_MESSAGE, auditParams.get(MSG_VALUE.getName()));
  }

  @Test
  void testConsumerRecordProcessingErrorWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);
    final Map<String, String> auditParams = makeAuditTest();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME_1, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_KEY.getName()));
    Assertions.assertEquals(TEST_VAL_STRING_HEADERS, auditParams.get(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MSG_VALUE.getName()));
  }

  @Test
  void testConsumerRecordProcessingErrorWithDisabledHeadersAndMessage() {
    final Map<String, String> auditParams = makeAuditTest();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME_1, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_KEY.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MSG_VALUE.getName()));
  }

  @Test
  void testConsumerRecordProcessingErrorWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TRIM_TO_LENGTH);
    final Map<String, String> auditParams = makeAuditTest();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME_1, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_STRING_HEADERS, TRIM_TO_LENGTH),
        auditParams.get(HEADERS.getName())
    );
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_KEY.getName()));
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_MESSAGE, TRIM_TO_LENGTH),
        auditParams.get(MSG_VALUE.getName())
    );
  }

  @NonNull
  private Map<String, String> makeAuditTest() {
    final ConsumerRecord<String, String> consumerRecord =
        TestUtils.createConsumerRecord(TEST_VAL_TOPIC_NAME_1, 0, TEST_VAL_MSG_ID, TEST_VAL_MESSAGE,
            TEST_VAL_HEADERS_WITH_MSG_ID);

    Mockito.doThrow(new RuntimeException("Consumer record to load msg conversion error"))
        .when(consumerRecordToQmngrLoadMsgDtoConverter)
        .convert(ArgumentMatchers.any(ConsumerRecord.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgListener.onMessage(consumerRecord);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Ошибка обработки сообщения из Kafka", auditMessageCaptor.getValue());

    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    Mockito.verify(loadMsgService, Mockito.times(0))
        .process(ArgumentMatchers.any(QmngrLoadMsgDto.class), ArgumentMatchers.any(MqType.class));

    return auditParamsCaptor.getValue().get();
  }

}
