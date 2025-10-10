package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.PROC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrReadMsgErrorAuditEvent.QMNGR_READ_MSG_ERROR_PROC_SUCCESS;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgErrorCall;

/**
 * Сервис аудита процесса установки ошибочного статуса обработки сообщения в SOFR QManager.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrReadMsgErrorAuditService {

  private final AuditEventSenderService auditEventSenderService;
  private final AuditEventParamsBuilderFactory auditEventParamsBuilderFactory;

  /**
   * Логирование вызова хранимой процедуры qmanager_read_msg_error.
   *
   * @param readMsgErrorCall контейнер с параметрами для вызова хранимой процедуры qmanager_read_msg_error
   */
  public void logReadMsgErrorCall(@NonNull QmngrReadMsgErrorCall readMsgErrorCall) {
    log.info(
        "Calling stored procedure = [{}] for message with id = [{}] with error state = [{}: {}]",
        readMsgErrorCall.getQmngrStoredProc(),
        readMsgErrorCall.getMsgId(),
        readMsgErrorCall.getErrorCode(),
        readMsgErrorCall.getErrorDesc()
    );
  }

  /**
   * Логирование и аудит ошибки вызова хранимой процедуры qmanager_read_msg_error.
   *
   * @param readMsgErrorCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg_error
   * @param errorCause       причина ошибки
   */
  public void auditReadMsgErrorCallError(@NonNull QmngrReadMsgErrorCall readMsgErrorCall,
                                         @NonNull String errorCause) {
    final QmngrStoredProc storedProc = readMsgErrorCall.getQmngrStoredProc();
    final String msgId = readMsgErrorCall.getMsgId();
    log.error(
        "Error while call stored procedure = [{}] for message with id = [{}]. Cause: {}.",
        storedProc,
        msgId,
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка вызова хранимой процедуры",
        QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, readMsgErrorCall.getErrorCode())
            .withParam(ERROR_DESC, readMsgErrorCall.getErrorDesc())
            .build()
    );
  }

  /**
   * Логирование и аудит успешного результата вызова хранимой процедуры qmanager_read_msg_error.
   *
   * @param readMsgErrorCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg_error
   */
  public void auditReadMsgErrorCallSuccessCompletion(@NonNull QmngrReadMsgErrorCall readMsgErrorCall) {
    final QmngrStoredProc storedProc = readMsgErrorCall.getQmngrStoredProc();
    final String msgId = readMsgErrorCall.getMsgId();
    log.info("Stored procedure = [{}] for message with id = [{}] completed successfully", storedProc, msgId);
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена успешно",
        QMNGR_READ_MSG_ERROR_PROC_SUCCESS,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, readMsgErrorCall.getErrorCode())
            .withParam(ERROR_DESC, readMsgErrorCall.getErrorDesc())
            .build()
    );
  }

}
