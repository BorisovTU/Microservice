package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_HEADERS_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.constants.QmngrConstants.DEFAULT_MONITORING_MESSAGE_MAX_LENGTH;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType.KAFKA;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CAUSE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.PROC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.MQ_READ_MSG_SENDING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.MQ_READ_MSG_SENDING_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_PROC_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_RECEIVED;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.UNKNOWN_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.READ_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_UNKNOWN_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_READ_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_READ_MSG_PROC_CALL_OUT_PARAMS_ERROR;
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
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestUtils;

@ExtendWith(MockitoExtension.class)
class QmngrReadMsgAuditServiceTest {

  private static final String MSG_RECEIVED_MESSAGE_FROM_QMNGR = "Получено сообщение из SOFR QManager";
  private static final String MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR =
      "Ошибка валидации сообщения из SOFR QManager";
  private static final String MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR =
      "Ошибка обработки сообщения из SOFR QManager";

  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);
  private final MonitoringParamHeadersAuditProperties monitoringParamHeadersAuditProperties =
      Mockito.mock(MonitoringParamHeadersAuditProperties.class);
  private final MonitoringParamMessageAuditProperties monitoringParamMessageAuditProperties =
      Mockito.mock(MonitoringParamMessageAuditProperties.class);
  @Captor
  protected ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  protected ArgumentCaptor<QmngrReadMsgAuditEvent> auditEventCaptor;
  @Captor
  protected ArgumentCaptor<Supplier<Map<String, String>>> auditParamsCaptor;

  private final QmngrReadMsgAuditService qmngrReadMsgAuditService;

  QmngrReadMsgAuditServiceTest() {
    qmngrReadMsgAuditService = new QmngrReadMsgAuditService(
        messageLoggingService,
        auditEventSenderService,
        new AuditEventParamsBuilderFactory(
            monitoringParamHeadersAuditProperties,
            monitoringParamMessageAuditProperties
        )
    );
  }

  @Test
  void testAuditReadMsgCallError() {
    qmngrReadMsgAuditService.auditReadMsgCallError(new QmngrReadMsgCall(), "Ошибка вызова хранимой процедуры");

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Ошибка вызова хранимой процедуры", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROC_CALL_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(1, auditParams.size());
    Assertions.assertEquals(READ_MSG.getName(), auditParams.get(PROC_NAME.getName()));
  }

  @Test
  void testAuditReadMsgCallUnknownStateCompletion() {
    final QmngrReadMsgCall qmngrReadMsgCall = new QmngrReadMsgCall();
    TestUtils.setQmngrStoredProcCallOutParams(qmngrReadMsgCall, TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_UNKNOWN_ERROR);

    qmngrReadMsgAuditService.auditReadMsgCallUnknownStateCompletion(qmngrReadMsgCall);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена с неизвестным статусом", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROC_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(READ_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(UNKNOWN_ERROR.getCode()), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(UNKNOWN_ERROR.getDescription(), auditParams.get(ERROR_DESC.getName()));
  }

  @Test
  void testAuditReadMsgCallErrorCompletion() {
    final QmngrReadMsgCall qmngrReadMsgCall = new QmngrReadMsgCall();
    TestUtils.setQmngrStoredProcCallOutParams(qmngrReadMsgCall, TEST_VAL_READ_MSG_PROC_CALL_OUT_PARAMS_ERROR);

    qmngrReadMsgAuditService.auditReadMsgCallErrorCompletion(qmngrReadMsgCall);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена с ошибкой", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROC_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(READ_MSG.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(TEST_VAL_ERROR_CODE), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(TEST_VAL_ERROR_DESC, auditParams.get(ERROR_DESC.getName()));
  }

  @Test
  void testAuditReadMsgValidationErrorWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgValidationError(
        TEST_VAL_READ_MSG.getMsgId(),
        TEST_VAL_READ_MSG.getTopic(),
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditReadMsgValidationErrorWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgValidationError(
        TEST_VAL_READ_MSG.getMsgId(),
        TEST_VAL_READ_MSG.getTopic(),
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditReadMsgValidationErrorWithDisabledHeadersAndMessage() {
    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgValidationError(
        TEST_VAL_READ_MSG.getMsgId(),
        TEST_VAL_READ_MSG.getTopic(),
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditReadMsgValidationErrorWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgValidationError(
        TEST_VAL_READ_MSG.getMsgId(),
            TEST_VAL_READ_MSG.getTopic(),
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(3, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_VALIDATION_ERROR, auditParams.get(ERROR_CAUSE.getName()));
  }

  @Test
  void testAuditReadMsgToMqSendingError() {
    qmngrReadMsgAuditService.auditReadMsgToMqSendingError(
        TEST_VAL_READ_MSG,
        "Ошибка отправки сообщения"
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Ошибка отправки сообщения", auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_READ_MSG_SENDING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(1, auditParams.size());
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
  }

  @Test
  void testAuditReadMsgToMqSendingSuccess() {
    qmngrReadMsgAuditService.auditReadMsgToMqSendingSuccess(TEST_VAL_READ_MSG);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Сообщение успешно отправлено", auditMessageCaptor.getValue());
    Assertions.assertEquals(MQ_READ_MSG_SENDING_SUCCESS, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(1, auditParams.size());
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
  }

  @Test
  void testAuditReadMsgProcessingErrorWithDisabledHeaders() {
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_MESSAGE_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgProcessingError(
        TEST_VAL_READ_MSG,
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
  }

  @Test
  void testAuditReadMsgProcessingErrorWithDisabledMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength())
        .thenReturn(DEFAULT_MONITORING_HEADERS_MAX_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgProcessingError(
        TEST_VAL_READ_MSG,
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditReadMsgProcessingErrorWithDisabledHeadersAndMessage() {
    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgProcessingError(
        TEST_VAL_READ_MSG,
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertFalse(auditParams.containsKey(HEADERS.getName()));
    Assertions.assertFalse(auditParams.containsKey(MESSAGE.getName()));
  }

  @Test
  void testAuditReadMsgProcessingErrorWithTrimHeadersAndMessage() {
    Mockito.when(monitoringParamHeadersAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamHeadersAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);
    Mockito.when(monitoringParamMessageAuditProperties.isEnabled()).thenReturn(true);
    Mockito.when(monitoringParamMessageAuditProperties.getMaxLength()).thenReturn(TEST_VAL_TRIM_TO_LENGTH);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    qmngrReadMsgAuditService.auditReadMsgProcessingError(
        TEST_VAL_READ_MSG,
        MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR
    );

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals(MSG_RECEIVED_MESSAGE_FROM_QMNGR_PROCESSING_ERROR, auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(2, auditParams.size());
    Assertions.assertEquals(TEST_VAL_TOPIC_NAME, auditParams.get(TOPIC.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
  }

}
