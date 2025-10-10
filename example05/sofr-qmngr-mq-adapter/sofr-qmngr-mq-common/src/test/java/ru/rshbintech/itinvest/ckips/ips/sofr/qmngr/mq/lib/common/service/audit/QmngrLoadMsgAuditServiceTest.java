package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_HEADERS_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType.KAFKA;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CAUSE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.PROC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_RECEIVED;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.LOAD_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_LOAD_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_STRING_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_TOPIC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_TRIM_TO_LENGTH;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestUtils;

@ExtendWith(MockitoExtension.class)
class QmngrLoadMsgAuditServiceTest {

  private static final String MSG_RECEIVED_MESSAGE_FROM_KAFKA = "Получено сообщение из Kafka";
  private static final String MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR = "Ошибка валидации сообщения из Kafka";
  private static final String MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR = "Ошибка обработки сообщения из Kafka";
  private static final String MSG_PROC_CALL_ERROR = "Ошибка вызова хранимой процедуры";

  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);
  private final MonitoringParamHeadersAuditProperties monitoringParamHeadersAuditProperties =
      Mockito.mock(MonitoringParamHeadersAuditProperties.class);
  private final MonitoringParamMessageAuditProperties monitoringParamMessageAuditProperties =
      Mockito.mock(MonitoringParamMessageAuditProperties.class);
  @Captor
  protected ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  protected ArgumentCaptor<QmngrLoadMsgAuditEvent> auditEventCaptor;
  @Captor
  protected ArgumentCaptor<Supplier<Map<String, String>>> auditParamsCaptor;

  private final QmngrLoadMsgAuditService qmngrLoadMsgAuditService;

  QmngrLoadMsgAuditServiceTest() {
    qmngrLoadMsgAuditService = new QmngrLoadMsgAuditService(
        messageLoggingService,
        auditEventSenderService,
        new AuditEventParamsBuilderFactory(
            monitoringParamHeadersAuditProperties,
            monitoringParamMessageAuditProperties
        )
    );
  }

  @Test
  void testAuditLoadMsgReceivedWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgReceived(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertEquals(TEST_VAL_MESSAGE, auditParams.get(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgReceivedWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgReceived(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_STRING_HEADERS, auditParams.get(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgReceivedWithDisabledHeadersAndMessage() {
    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgReceived(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgReceivedWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgReceived(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(5, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_STRING_HEADERS, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(HEADERS.getName())
    );
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_MESSAGE, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(MESSAGE.getName())
    );
  }

  @Test
  void testAuditLoadMsgValidationErrorWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgValidationError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(5, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertEquals(TEST_VAL_MESSAGE, auditParams.get(MESSAGE.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditLoadMsgValidationErrorWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgValidationError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(5, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_STRING_HEADERS, auditParams.get(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditLoadMsgValidationErrorWithDisabledHeadersAndMessage() {
    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgValidationError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditLoadMsgValidationErrorWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgValidationError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(6, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_STRING_HEADERS, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(HEADERS.getName())
    );
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_MESSAGE, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(MESSAGE.getName())
    );
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditLoadMsgProcessingErrorWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgProcessingError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertEquals(TEST_VAL_MESSAGE, auditParams.get(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgProcessingErrorWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgProcessingError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_STRING_HEADERS, auditParams.get(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgProcessingErrorWithDisabledHeadersAndMessage() {
    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgProcessingError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditLoadMsgProcessingErrorWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrLoadMsgAuditService.auditLoadMsgProcessingError(
        TEST_VAL_LOAD_MSG,
        KAFKA,
        MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_KAFKA_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(5, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ESB_DT), auditParams.get(ESB_DT.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_STRING_HEADERS, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(HEADERS.getName())
    );
    Assertions.assertEquals(
        StringUtils.abbreviate(TEST_VAL_MESSAGE, TEST_VAL_TRIM_TO_LENGTH),
        auditParams.get(MESSAGE.getName())
    );
  }

  @Test
  void testAuditLoadMsgCallError() {
    qmngrLoadMsgAuditService.auditLoadMsgCallError(new QmngrLoadMsgCall(TEST_VAL_LOAD_MSG), KAFKA, MSG_PROC_CALL_ERROR);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_PROC_CALL_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_CALL_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(LOAD_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
  }

  @Test
  void testAuditLoadMsgCallUnknownStateCompletion() {
    final QmngrLoadMsgCall loadMsgCall = new QmngrLoadMsgCall(TEST_VAL_LOAD_MSG);
    TestUtils.setQmngrStoredProcCallOutParams(loadMsgCall, TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR);

    qmngrLoadMsgAuditService.auditLoadMsgCallUnknownStateCompletion(loadMsgCall, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена с неизвестным статусом", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(LOAD_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ERROR_CODE), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(TEST_VAL_ERROR_DESC, auditParams.get(ERROR_DESC.getName()));
  }

  @Test
  void testAuditLoadMsgCallErrorCompletion() {
    final QmngrLoadMsgCall loadMsgCall = new QmngrLoadMsgCall(TEST_VAL_LOAD_MSG);
    TestUtils.setQmngrStoredProcCallOutParams(loadMsgCall, TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR);

    qmngrLoadMsgAuditService.auditLoadMsgCallErrorCompletion(loadMsgCall, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена с ошибкой", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(LOAD_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ERROR_CODE), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(TEST_VAL_ERROR_DESC, auditParams.get(ERROR_DESC.getName()));
  }

  @Test
  void testAuditLoadMsgCallSuccessCompletion() {
    final QmngrLoadMsgCall loadMsgCall = new QmngrLoadMsgCall(TEST_VAL_LOAD_MSG);
    TestUtils.setQmngrStoredProcCallOutParams(loadMsgCall, TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR);

    qmngrLoadMsgAuditService.auditLoadMsgCallSuccessCompletion(loadMsgCall, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена успешно", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_SUCCESS, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(LOAD_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
  }

}
