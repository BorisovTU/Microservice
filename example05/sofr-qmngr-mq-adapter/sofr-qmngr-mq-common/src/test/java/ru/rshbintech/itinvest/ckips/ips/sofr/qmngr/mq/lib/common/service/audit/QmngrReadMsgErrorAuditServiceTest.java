package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.PROC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_SUCCESS;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrReadMsgError.UNKNOWN_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc.READ_MSG_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.utils.TestConstants.TEST_VAL_READ_MSG;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamHeadersAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties.monitoring.MonitoringParamMessageAuditProperties;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;

@ExtendWith(MockitoExtension.class)
class QmngrReadMsgErrorAuditServiceTest {

  private static final QmngrReadMsgErrorCall TEST_VAL_PROC_CALL = new QmngrReadMsgErrorCall(
      TEST_VAL_READ_MSG,
      UNKNOWN_ERROR
  );

  private final AuditEventSenderService auditEventSenderService = Mockito.mock(AuditEventSenderService.class);

  @Captor
  protected ArgumentCaptor<String> auditMessageCaptor;
  @Captor
  protected ArgumentCaptor<QmngrReadMsgErrorAuditEvent> auditEventCaptor;
  @Captor
  protected ArgumentCaptor<Supplier<Map<String, String>>> auditParamsCaptor;

  private final QmngrReadMsgErrorAuditService qmngrReadMsgErrorAuditService;

  QmngrReadMsgErrorAuditServiceTest() {
    qmngrReadMsgErrorAuditService = new QmngrReadMsgErrorAuditService(
        auditEventSenderService,
        new AuditEventParamsBuilderFactory(
            Mockito.mock(MonitoringParamHeadersAuditProperties.class),
            Mockito.mock(MonitoringParamMessageAuditProperties.class)
        )
    );
  }

  @Test
  void testAuditReadMsgErrorCallError() {
    qmngrReadMsgErrorAuditService.auditReadMsgErrorCallError(TEST_VAL_PROC_CALL, "Ошибка вызова хранимой процедуры");

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Ошибка вызова хранимой процедуры", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(READ_MSG_ERROR.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(UNKNOWN_ERROR.getCode()), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(UNKNOWN_ERROR.getDescription(), auditParams.get(ERROR_DESC.getName()));
  }

  @Test
  void testAuditReadMsgErrorCallSuccessCompletion() {
    qmngrReadMsgErrorAuditService.auditReadMsgErrorCallSuccessCompletion(TEST_VAL_PROC_CALL);

    Mockito.verify(auditEventSenderService, Mockito.times(1)).sendAuditEvent(
        auditMessageCaptor.capture(),
        auditEventCaptor.capture(),
        auditParamsCaptor.capture()
    );

    Assertions.assertEquals("Хранимая процедура завершена успешно", auditMessageCaptor.getValue());
    Assertions.assertEquals(QMNGR_READ_MSG_ERROR_PROC_SUCCESS, auditEventCaptor.getValue());

    final Map<String, String> auditParams = auditParamsCaptor.getValue().get();
    Assertions.assertEquals(4, auditParams.size());
    Assertions.assertEquals(READ_MSG_ERROR.getName(), auditParams.get(PROC_NAME.getName()));
    Assertions.assertEquals(TEST_VAL_MSG_ID, auditParams.get(MSG_ID.getName()));
    Assertions.assertEquals(Objects.toString(UNKNOWN_ERROR.getCode()), auditParams.get(ERROR_CODE.getName()));
    Assertions.assertEquals(UNKNOWN_ERROR.getDescription(), auditParams.get(ERROR_DESC.getName()));
  }

}
