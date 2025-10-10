package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doThrow;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState.CONTINUE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState.STOP_NORMALLY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.MQ_READ_MSG_SENDING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.MQ_READ_MSG_SENDING_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_PROC_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgAuditEvent.QMNGR_READ_MSG_VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcErrorCode.NO_MESSAGES;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import java.sql.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialClob;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.SendResult;
import reactor.core.publisher.Mono;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.mdc.MdcAdapter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.ValidationResult;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.QmngrReadMsgProcessState;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgSendToMqException;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.exception.QmngrReadMsgTechnicalException;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.QmngrReadMsgToMqSendingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventParamsBuilderFactory;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.AuditEventSenderService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrReadMsgAuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrReadMsgErrorAuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.validation.GenericValidator;

@ExtendWith(MockitoExtension.class)
class QmngrReadMsgServiceTest {

  private final QmngrDao qmngrDao = Mockito.mock(QmngrDao.class);
  private final DataSource dataSource = Mockito.mock(DataSource.class);
  private final GenericValidator validator = Mockito.mock(GenericValidator.class);
  private final MessageLoggingService messageLoggingService = Mockito.mock(MessageLoggingService.class);
  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);
  protected final QmngrReadMsgToMqSendingService readMsgToMqSendingService =
          Mockito.mock(QmngrReadMsgToMqSendingService.class);

  private JsonFactory jsonFactory = JsonFactory.builder()
          .build();
  private ObjectMapper objectMapper = new ObjectMapper();

  @Captor
  protected ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  protected ArgumentCaptor<AuditEvent> auditEventCaptor;
  @Captor
  protected ArgumentCaptor<QmngrReadMsgDto> readMsgToMqSendingServiceCaptor;
  @Captor
  protected ArgumentCaptor<QmngrReadMsgErrorCall> readMsgErrorStoredProcCallCaptor;

  private final QmngrReadMsgService readMsgService;

  QmngrReadMsgServiceTest() {
    final AuditEventParamsBuilderFactory auditEventParamsBuilderFactory =
            Mockito.mock(AuditEventParamsBuilderFactory.class);
    readMsgService = new QmngrReadMsgService(
            dataSource,
            Mockito.mock(MdcAdapter.class),
            validator,
            new QmngrReadMsgAuditService(
                    messageLoggingService,
                    auditEventSenderService,
                    auditEventParamsBuilderFactory
            ),
            new QmngrReadMsgErrorService(
                    qmngrDao,
                    new QmngrReadMsgErrorAuditService(auditEventSenderService, auditEventParamsBuilderFactory),
                    jsonFactory,
                    objectMapper,
                    readMsgToMqSendingService
            ),
            readMsgToMqSendingService
    );
  }

  @Test
  void testProcCallError() throws QmngrReadMsgTechnicalException, SQLException {
    Mockito.when(dataSource.getConnection())
            .thenThrow(new RuntimeException(QMNGR_READ_MSG_PROC_CALL_ERROR.name()));

    readMsgService.processSingle();

    Mockito.verify(auditEventSenderService, Mockito.times(1))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Ошибка вызова хранимой процедуры", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_PROC_CALL_ERROR, auditEventCaptor.getValue());
  }

  @Test
  void testProcCallWithErrorStateCompletion() throws QmngrReadMsgTechnicalException, SQLException {
    Connection mockConnection = mock(Connection.class);
    CallableStatement mockStatement = mock(CallableStatement.class);
    Clob fakeClob = new javax.sql.rowset.serial.SerialClob("Test message".toCharArray());

    when(dataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareCall(anyString())).thenReturn(mockStatement);

    when(mockStatement.getString(eq(2))).thenReturn("someTopic");
    when(mockStatement.getString(eq(3))).thenReturn("someMsgId");
    when(mockStatement.getString(eq(4))).thenReturn("{\"header\": \"value\"}");
    when(mockStatement.getClob(eq(5))).thenReturn(fakeClob);
    when(mockStatement.getInt(eq(6))).thenReturn(1001);
    when(mockStatement.getString(eq(7))).thenReturn("Simulated error");

    final QmngrReadMsgProcessState processState = readMsgService.processSingle();

    Assertions.assertEquals(CONTINUE, processState);

    Mockito.verify(auditEventSenderService, Mockito.times(1))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Хранимая процедура завершена с ошибкой", auditMessageCaptor.getValue());

    Assertions.assertEquals(QMNGR_READ_MSG_PROC_ERROR, auditEventCaptor.getValue());
  }

  @Test
  void testProcCallWithNoMessagesStateCompletion() throws QmngrReadMsgTechnicalException, SQLException {
    Connection mockConnection = Mockito.mock(Connection.class);
    CallableStatement mockCallableStatement = Mockito.mock(CallableStatement.class);

    Mockito.when(dataSource.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);

    Mockito.when(mockCallableStatement.getString(Mockito.eq(2))).thenReturn(null);
    Mockito.when(mockCallableStatement.getString(Mockito.eq(3))).thenReturn(null);
    Mockito.when(mockCallableStatement.getString(Mockito.eq(4))).thenReturn(null);
    Mockito.when(mockCallableStatement.getClob(Mockito.eq(5))).thenReturn(null);
    Mockito.when(mockCallableStatement.getInt(Mockito.eq(6))).thenReturn(NO_MESSAGES.getCode());
    Mockito.when(mockCallableStatement.getString(Mockito.eq(7))).thenReturn("");


    final QmngrReadMsgProcessState processState = readMsgService.processSingle();

    Assertions.assertEquals(STOP_NORMALLY, processState);

    Mockito.verify(auditEventSenderService, Mockito.times(0))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );
  }

    @Test
  void testValidationErrorAndReadMsgErrorProcCallSuccess() throws QmngrReadMsgTechnicalException, SQLException {
    Connection mockConnection = mock(Connection.class);
    CallableStatement mockStmt = mock(CallableStatement.class);
    Clob fakeClob = new SerialClob(TEST_VAL_MESSAGE.toCharArray());
    when(dataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareCall(anyString())).thenReturn(mockStmt);
    when(mockStmt.getString(eq(2))).thenReturn(TEST_VAL_TOPIC_NAME);
    when(mockStmt.getString(eq(3))).thenReturn(TEST_VAL_MSG_ID);
    when(mockStmt.getString(eq(4))).thenReturn(TEST_VAL_STRING_HEADERS);
    when(mockStmt.getClob(eq(5))).thenReturn(fakeClob);
    when(mockStmt.getInt(eq(6))).thenReturn(0);
    when(mockStmt.getString(eq(7))).thenReturn(EMPTY);

    final ValidationResult vr = ValidationResult.builder()
            .valid(false)
            .errorMsg("Ошибка валидации сообщения из SOFR QManager")
            .build();
    when(validator.validate(any())).thenReturn(vr);
    when(messageLoggingService.getLogMessageText(any(), any())).thenReturn(EMPTY);

    final QmngrReadMsgProcessState processState = readMsgService.processSingle();

    Assertions.assertEquals(CONTINUE, processState);

    Mockito.verify(auditEventSenderService, Mockito.times(2))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Ошибка валидации сообщения из SOFR QManager", auditMessageCaptor.getAllValues()
            .get(0));
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getAllValues()
            .get(0));

    Mockito.verify(qmngrDao, Mockito.times(1))
            .callReadMsgError(readMsgErrorStoredProcCallCaptor.capture());

    final QmngrReadMsgErrorCall readMsgErrorCall = readMsgErrorStoredProcCallCaptor.getValue();
    Assertions.assertEquals(
            TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_VALIDATION_ERROR.size(), readMsgErrorCall.getInParams()
                    .size());
    Assertions.assertTrue(
            Maps.difference(readMsgErrorCall.getInParams(), TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_VALIDATION_ERROR)
                    .areEqual()
    );

    Mockito.verify(auditEventSenderService, Mockito.times(2))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Хранимая процедура завершена успешно", auditMessageCaptor.getAllValues()
            .get(1));
    Assertions.assertEquals(QMNGR_READ_MSG_ERROR_PROC_SUCCESS, auditEventCaptor.getAllValues()
            .get(1));
  }

  @Test
  void testValidationErrorAndReadMsgErrorProcCallError() throws QmngrReadMsgTechnicalException, SQLException {
    Connection mockConnection = mock(Connection.class);
    CallableStatement mockStmt = mock(CallableStatement.class);
    Clob fakeClob = new SerialClob(TEST_VAL_MESSAGE.toCharArray());
    when(dataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.prepareCall(anyString())).thenReturn(mockStmt);
    when(mockStmt.getString(eq(2))).thenReturn(TEST_VAL_TOPIC_NAME);
    when(mockStmt.getString(eq(3))).thenReturn(TEST_VAL_MSG_ID);
    when(mockStmt.getString(eq(4))).thenReturn(TEST_VAL_STRING_HEADERS);
    when(mockStmt.getClob(eq(5))).thenReturn(fakeClob);
    when(mockStmt.getInt(eq(6))).thenReturn(0);
    when(mockStmt.getString(eq(7))).thenReturn(EMPTY);

    ValidationResult vrError = ValidationResult.builder()
            .valid(false)
            .errorMsg("Ошибка валидации сообщения из SOFR QManager")
            .build();
    when(validator.validate(any())).thenReturn(vrError);
    when(messageLoggingService.getLogMessageText(any(), any())).thenReturn(EMPTY);

    doThrow(new RuntimeException(QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR.name()))
            .when(qmngrDao).callReadMsgError(any(QmngrReadMsgErrorCall.class));


    final QmngrReadMsgProcessState processState = readMsgService.processSingle();

    Assertions.assertEquals(CONTINUE, processState);

    Mockito.verify(auditEventSenderService, Mockito.times(2))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Ошибка валидации сообщения из SOFR QManager", auditMessageCaptor.getAllValues()
            .get(0));
    Assertions.assertEquals(QMNGR_READ_MSG_VALIDATION_ERROR, auditEventCaptor.getAllValues()
            .get(0));

    Mockito.verify(qmngrDao, Mockito.times(1))
            .callReadMsgError(readMsgErrorStoredProcCallCaptor.capture());

    final QmngrReadMsgErrorCall readMsgErrorCall = readMsgErrorStoredProcCallCaptor.getValue();
    Assertions.assertEquals(
            TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_VALIDATION_ERROR.size(), readMsgErrorCall.getInParams()
                    .size());
    Assertions.assertTrue(
            Maps.difference(readMsgErrorCall.getInParams(), TEST_VAL_PROC_ERROR_CALL_IN_PARAMS_WITH_VALIDATION_ERROR)
                    .areEqual()
    );

    Mockito.verify(auditEventSenderService, Mockito.times(2))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    Assertions.assertEquals("Ошибка вызова хранимой процедуры", auditMessageCaptor.getAllValues()
            .get(1));
    Assertions.assertEquals(QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR, auditEventCaptor.getAllValues()
            .get(1));
  }

  @Test
  void testProcessingError() throws QmngrReadMsgTechnicalException, SQLException {
    when(validator.validate(any())).thenReturn(ValidationResult.ok());

    Connection mockConnection = Mockito.mock(Connection.class);
    CallableStatement mockCallableStatement = Mockito.mock(CallableStatement.class);

    Mockito.when(dataSource.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);

    Clob fakeClob = new SerialClob(TEST_VAL_MESSAGE.toCharArray());
    when(mockCallableStatement.getString(2)).thenReturn(TEST_VAL_TOPIC_NAME);
    when(mockCallableStatement.getString(3)).thenReturn(TEST_VAL_MSG_ID);
    when(mockCallableStatement.getString(4)).thenReturn(TEST_VAL_STRING_HEADERS);
    when(mockCallableStatement.getClob(5)).thenReturn(fakeClob);
    when(mockCallableStatement.getInt(6)).thenReturn(-1);
    when(mockCallableStatement.getString(7)).thenReturn(TEST_VAL_ERROR_DESC);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
            .thenReturn(EMPTY);

    readMsgService.processSingle();

    Mockito.verify(auditEventSenderService, Mockito.times(1))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Хранимая процедура завершена с ошибкой", capturedAuditMessages.get(0));

    final List<AuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(QMNGR_READ_MSG_PROC_ERROR, capturedAuditEvents.get(0));
  }

  @Test
  void testSendingError() throws QmngrReadMsgSendToMqException, QmngrReadMsgTechnicalException, SQLException {
    when(validator.validate(any())).thenReturn(ValidationResult.ok());

    Connection mockConnection = Mockito.mock(Connection.class);
    CallableStatement mockCallableStatement = Mockito.mock(CallableStatement.class);

    Mockito.when(dataSource.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);

    Clob fakeClob = new SerialClob(TEST_VAL_MESSAGE.toCharArray());
    when(mockCallableStatement.getString(2)).thenReturn(TEST_VAL_TOPIC_NAME);
    when(mockCallableStatement.getString(3)).thenReturn(TEST_VAL_MSG_ID);
    when(mockCallableStatement.getString(4)).thenReturn(TEST_VAL_STRING_HEADERS);
    when(mockCallableStatement.getClob(5)).thenReturn(fakeClob);
    when(mockCallableStatement.getInt(6)).thenReturn(0);
    when(mockCallableStatement.getString(7)).thenReturn(TEST_VAL_ERROR_DESC);

    CompletableFuture<SendResult<String, String>> failingFuture = new CompletableFuture<>();
    failingFuture.completeExceptionally(new QmngrReadMsgSendToMqException(new RuntimeException(MQ_READ_MSG_SENDING_ERROR.name())));
    when(readMsgToMqSendingService.sendAsync(ArgumentMatchers.any(QmngrReadMsgDto.class)))
            .thenReturn(Mono.fromFuture(failingFuture));

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
            .thenReturn(EMPTY);

    readMsgService.processSingle();

    Mockito.verify(auditEventSenderService, Mockito.times(1))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Ошибка отправки сообщения", capturedAuditMessages.get(0));

    final List<AuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_READ_MSG_SENDING_ERROR, capturedAuditEvents.get(0));
  }

  @Test
  void testReadSuccessSending() throws QmngrReadMsgTechnicalException, SQLException, QmngrReadMsgSendToMqException {
    when(validator.validate(any())).thenReturn(ValidationResult.ok());

    Connection mockConnection = Mockito.mock(Connection.class);
    CallableStatement mockCallableStatement = Mockito.mock(CallableStatement.class);

    Mockito.when(dataSource.getConnection()).thenReturn(mockConnection);
    Mockito.when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);

    Clob fakeClob = new SerialClob(TEST_VAL_MESSAGE.toCharArray());
    when(mockCallableStatement.getString(2)).thenReturn(TEST_VAL_TOPIC_NAME);
    when(mockCallableStatement.getString(3)).thenReturn(TEST_VAL_MSG_ID);
    when(mockCallableStatement.getString(4)).thenReturn(TEST_VAL_STRING_HEADERS);
    when(mockCallableStatement.getClob(5)).thenReturn(fakeClob);
    when(mockCallableStatement.getInt(6)).thenReturn(0);
    when(mockCallableStatement.getString(7)).thenReturn(TEST_VAL_ERROR_DESC);

    Mockito.when(messageLoggingService.getLogMessageText(ArgumentMatchers.any(), ArgumentMatchers.any()))
            .thenReturn(EMPTY);

    SendResult<String, String> fakeSendResult = new SendResult<>(new ProducerRecord<>(TEST_VAL_TOPIC_NAME, TEST_VAL_MSG_ID), null);

    CompletableFuture<SendResult<String, String>> successfulFuture = new CompletableFuture<>();
    successfulFuture.complete(fakeSendResult);
    when(readMsgToMqSendingService.sendAsync(ArgumentMatchers.any(QmngrReadMsgDto.class)))
            .thenReturn(Mono.fromFuture(successfulFuture));

    final QmngrReadMsgProcessState processState = readMsgService.processSingle();

    Assertions.assertEquals(CONTINUE, processState);

    Mockito.verify(readMsgToMqSendingService, Mockito.times(1))
            .sendAsync(ArgumentMatchers.any(QmngrReadMsgDto.class));

    Mockito.verify(auditEventSenderService, Mockito.times(1))
            .sendAuditEvent(
                    auditMessageCaptor.capture(),
                    auditEventCaptor.capture(),
                    ArgumentMatchers.any()
            );

    final List<String> capturedAuditMessages = auditMessageCaptor.getAllValues();
    Assertions.assertEquals("Сообщение успешно отправлено", capturedAuditMessages.get(0));

    final List<AuditEvent> capturedAuditEvents = auditEventCaptor.getAllValues();
    Assertions.assertEquals(MQ_READ_MSG_SENDING_SUCCESS, capturedAuditEvents.get(0));
  }

}
