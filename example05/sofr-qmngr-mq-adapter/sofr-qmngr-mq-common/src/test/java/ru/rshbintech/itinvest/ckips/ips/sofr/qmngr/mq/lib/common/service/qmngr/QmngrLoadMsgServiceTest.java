package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType.KAFKA;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_RECEIVED;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_LOAD_MSG;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MESSAGE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_STRING_HEADERS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_TOPIC_NAME;

import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.mdc.MdcAdapter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.ValidationResult;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventParamsBuilderFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventSenderService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrLoadMsgAuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.QmngrProcOutParamsAnswer;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.validation.GenericValidator;

@ExtendWith(MockitoExtension.class)
class QmngrLoadMsgServiceTest {

  private static final Map<String, Object> TEST_VAL_PROC_CALL_IN_PARAMS = Map.of(
      TOPIC.getName(), TEST_VAL_TOPIC_NAME,
      MSG_ID.getName(), TEST_VAL_MSG_ID,
      ESB_DT.getName(), TEST_VAL_ESB_DT,
      HEADERS.getName(), TEST_VAL_STRING_HEADERS,
      MESSAGE.getName(), TEST_VAL_MESSAGE
  );

  private final QmngrDao qmngrDao = Mockito.mock(QmngrDao.class);
  private final GenericValidator validator = Mockito.mock(GenericValidator.class);
  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);

  @Captor
  protected ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  protected ArgumentCaptor<QmngrLoadMsgAuditEvent> auditEventCaptor;
  @Captor
  protected ArgumentCaptor<QmngrLoadMsgCall> loadMsgStoredProcCallCaptor;

  private final QmngrLoadMsgService loadMsgService;

  QmngrLoadMsgServiceTest() {
    loadMsgService = new QmngrLoadMsgService(
        qmngrDao,
        Mockito.mock(MdcAdapter.class),
        validator,
        new QmngrLoadMsgAuditService(
            messageLoggingService,
            auditEventSenderService,
            Mockito.mock(AuditEventParamsBuilderFactory.class)
        )
    );
  }

  @Test
  void testValidationError() {
    final ValidationResult validationResult = ValidationResult.builder()
        .valid(false)
        .errorMsg("Ошибка валидации сообщения из Kafka")
        .build();

    Mockito.when(validator.validate(ArgumentMatchers.any(QmngrLoadMsgDto.class))).thenReturn(validationResult);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    Assertions.assertEquals("Ошибка валидации сообщения из Kafka", auditMessageCaptor.getValue());

    Assertions.assertEquals(MQ_LOAD_MSG_VALIDATION_ERROR, auditEventCaptor.getValue());

    Mockito.verify(qmngrDao, Mockito.times(0))
        .callLoadMsg(ArgumentMatchers.any(QmngrLoadMsgCall.class));
  }

  @Test
  void testProcessingError() {
    Mockito.doThrow(new RuntimeException(MQ_LOAD_MSG_PROCESSING_ERROR.name()))
        .when(validator)
        .validate(ArgumentMatchers.any(QmngrLoadMsgDto.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    Assertions.assertEquals("Ошибка обработки сообщения из Kafka", auditMessageCaptor.getValue());

    Assertions.assertEquals(MQ_LOAD_MSG_PROCESSING_ERROR, auditEventCaptor.getValue());

    Mockito.verify(qmngrDao, Mockito.times(0))
        .callLoadMsg(ArgumentMatchers.any(QmngrLoadMsgCall.class));
  }

  @Test
  void testProcCallWithSuccessStateCompletion() {
    Mockito.when(validator.validate(ArgumentMatchers.any(QmngrLoadMsgDto.class))).thenReturn(ValidationResult.ok());

    Mockito.doAnswer(new QmngrProcOutParamsAnswer(TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_SUCCESS))
        .when(qmngrDao)
        .callLoadMsg(ArgumentMatchers.any(QmngrLoadMsgCall.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(2)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Получено сообщение из Kafka", capturedAuditMessages.get(0));
    Assertions.assertEquals("Хранимая процедура завершена успешно", capturedAuditMessages.get(1));

    final List<QmngrLoadMsgAuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, capturedAuditEvents.get(0));
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_SUCCESS, capturedAuditEvents.get(1));

    Mockito.verify(qmngrDao, Mockito.times(1)).callLoadMsg(loadMsgStoredProcCallCaptor.capture());

    final QmngrLoadMsgCall loadMsgCall = loadMsgStoredProcCallCaptor.getValue();
    Assertions.assertEquals(TEST_VAL_PROC_CALL_IN_PARAMS.size(), loadMsgCall.getInParams().size());
    Assertions.assertTrue(Maps.difference(loadMsgCall.getInParams(), TEST_VAL_PROC_CALL_IN_PARAMS).areEqual());
  }

  @Test
  void testProcCallWithErrorStateCompletion() {
    Mockito.when(validator.validate(ArgumentMatchers.any(QmngrLoadMsgDto.class))).thenReturn(ValidationResult.ok());

    Mockito.doAnswer(new QmngrProcOutParamsAnswer(TEST_VAL_LOAD_MSG_PROC_CALL_OUT_PARAMS_ERROR))
        .when(qmngrDao)
        .callLoadMsg(ArgumentMatchers.any(QmngrLoadMsgCall.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(2)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Получено сообщение из Kafka", capturedAuditMessages.get(0));
    Assertions.assertEquals("Хранимая процедура завершена с ошибкой", capturedAuditMessages.get(1));

    final List<QmngrLoadMsgAuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, capturedAuditEvents.get(0));
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_ERROR, capturedAuditEvents.get(1));

    Mockito.verify(qmngrDao, Mockito.times(1)).callLoadMsg(loadMsgStoredProcCallCaptor.capture());

    final QmngrLoadMsgCall loadMsgCall = loadMsgStoredProcCallCaptor.getValue();
    Assertions.assertEquals(TEST_VAL_PROC_CALL_IN_PARAMS.size(), loadMsgCall.getInParams().size());
    Assertions.assertTrue(Maps.difference(loadMsgCall.getInParams(), TEST_VAL_PROC_CALL_IN_PARAMS).areEqual());
  }

  @Test
  void testProcCallWithUnknownStateCompletion() {
    Mockito.when(validator.validate(ArgumentMatchers.any(QmngrLoadMsgDto.class))).thenReturn(ValidationResult.ok());

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(2)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Получено сообщение из Kafka", capturedAuditMessages.get(0));
    Assertions.assertEquals("Хранимая процедура завершена с неизвестным статусом", capturedAuditMessages.get(1));

    final List<QmngrLoadMsgAuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, capturedAuditEvents.get(0));
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_ERROR, capturedAuditEvents.get(1));

    Mockito.verify(qmngrDao, Mockito.times(1)).callLoadMsg(loadMsgStoredProcCallCaptor.capture());

    final QmngrLoadMsgCall loadMsgCall = loadMsgStoredProcCallCaptor.getValue();
    Assertions.assertEquals(TEST_VAL_PROC_CALL_IN_PARAMS.size(), loadMsgCall.getInParams().size());
    Assertions.assertTrue(Maps.difference(loadMsgCall.getInParams(), TEST_VAL_PROC_CALL_IN_PARAMS).areEqual());
  }

  @Test
  void testProcCallError() {
    Mockito.when(validator.validate(ArgumentMatchers.any(QmngrLoadMsgDto.class))).thenReturn(ValidationResult.ok());

    Mockito.doThrow(new RuntimeException(QMNGR_LOAD_MSG_PROC_CALL_ERROR.name()))
        .when(qmngrDao)
        .callLoadMsg(ArgumentMatchers.any(QmngrLoadMsgCall.class));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
        .thenReturn(EMPTY);

    loadMsgService.process(TEST_VAL_LOAD_MSG, KAFKA);

    Mockito.verify(auditEventSenderService, Mockito.times(2)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        ArgumentMatchers.any()
    );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Получено сообщение из Kafka", capturedAuditMessages.get(0));
    Assertions.assertEquals("Ошибка вызова хранимой процедуры", capturedAuditMessages.get(1));

    final List<QmngrLoadMsgAuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_LOAD_MSG_RECEIVED, capturedAuditEvents.get(0));
    Assertions.assertEquals(QMNGR_LOAD_MSG_PROC_CALL_ERROR, capturedAuditEvents.get(1));
  }

}
