package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CAUSE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
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

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrReadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrReadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;

/**
 * Сервис аудита процесса получения сообщений из SOFR QManager с последующей их отправкой в MQ.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrReadMsgAuditService {

  private final MessageLoggingService messageLoggingService;
  private final AuditEventSenderService auditEventSenderService;
  private final AuditEventParamsBuilderFactory auditEventParamsBuilderFactory;

  /**
   * Логирование вызова хранимой процедуры qmanager_read_msg.
   *
   * @param readMsgCall контейнер с параметрами для вызова хранимой процедуры qmanager_read_msg
   */
  public void logReadMsgCall(@NonNull QmngrReadMsgCall readMsgCall) {
    log.info("Calling stored procedure = [{}]", readMsgCall.getQmngrStoredProc());
  }

  /**
   * Логирование и аудит ошибки вызова хранимой процедуры qmanager_read_msg.
   *
   * @param readMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg
   * @param errorCause  причина ошибки
   */
  public void auditReadMsgCallError(@NonNull QmngrReadMsgCall readMsgCall,
                                    @NonNull String errorCause) {
    final QmngrStoredProc storedProc = readMsgCall.getQmngrStoredProc();
    log.error(
        "Error while call stored procedure = [{}]. Cause: {}.",
        storedProc,
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка вызова хранимой процедуры",
        QMNGR_READ_MSG_PROC_CALL_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .build()
    );
  }

  /**
   * Логирование и аудит результата вызова хранимой процедуры qmanager_read_msg с неизвестным статусом.
   *
   * @param readMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg
   */
  public void auditReadMsgCallUnknownStateCompletion(@NonNull QmngrReadMsgCall readMsgCall) {
    final QmngrStoredProc storedProc = readMsgCall.getQmngrStoredProc();
    final String msgId = readMsgCall.getMsgId();
    final String errorCode = Objects.toString(readMsgCall.getErrorCode());
    final String errorDesc = readMsgCall.getErrorDesc();
    log.error(
        "Stored procedure = [{}]{} completed with unknown state = [{}{}]",
        storedProc,
        StringUtils.isNotBlank(msgId) ? " for message with id = [" + msgId + "]" : EMPTY,
        errorCode,
        StringUtils.isNotBlank(errorDesc) ? ": " + errorDesc : EMPTY
    );
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена с неизвестным статусом",
        QMNGR_READ_MSG_PROC_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, errorCode)
            .withParam(ERROR_DESC, errorDesc)
            .build()
    );
  }

  /**
   * Логирование и аудит результата вызова хранимой процедуры qmanager_read_msg со статусом 'нет сообщений'.
   *
   * @param readMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg
   */
  public void logReadMsgCallNoMessagesStateCompletion(@NonNull QmngrReadMsgCall readMsgCall) {
    final String errorDesc = readMsgCall.getErrorDesc();
    log.info(
        "Stored procedure = [{}] completed with no messages state = [{}{}]",
        readMsgCall.getQmngrStoredProc(),
        readMsgCall.getErrorCode(),
        StringUtils.isNotBlank(errorDesc) ? ": " + errorDesc : EMPTY
    );
  }

  /**
   * Логирование и аудит результата вызова хранимой процедуры qmanager_read_msg с ошибочным статусом.
   *
   * @param readMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_read_msg
   */
  public void auditReadMsgCallErrorCompletion(@NonNull QmngrReadMsgCall readMsgCall) {
    final QmngrStoredProc storedProc = readMsgCall.getQmngrStoredProc();
    final String msgId = readMsgCall.getMsgId();
    final Integer errorCode = readMsgCall.getErrorCode();
    final String errorDesc = readMsgCall.getErrorDesc();
    log.error(
        "Stored procedure = [{}]{} completed with error state = [{}{}]",
        storedProc,
        StringUtils.isNotBlank(msgId) ? " for message with id = [" + msgId + "]" : EMPTY,
        errorCode,
        StringUtils.isNotBlank(errorDesc) ? ": " + errorDesc : EMPTY
    );
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена с ошибкой",
        QMNGR_READ_MSG_PROC_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, errorCode)
            .withParam(ERROR_DESC, errorDesc)
            .build()
    );
  }

  /**
   * Логирование и аудит успешной загрузки сообщения из SOFR QManager.
   *
   * @param msgId контейнер с информацией о сообщении из SOFR QManager для отправки в MQ
   */
  public void auditReadMsgReceived(String msgId) {
    log.info(
        "Loaded read msg with ID = {} from SOFR QManager",
        msgId
    );
  }

  /**
   * Логирование и аудит ошибки валидации загруженного из SOFR QManager сообщения.
   *
   * @param msgId id сообщения из процедуры
   * @param validationErrorMessage причина ошибки
   */
  public void auditReadMsgValidationError(String msgId, String topic, String validationErrorMessage) {
    log.error(
        "Loaded read msg = {} from SOFR QManager has validation errors:\n{}",
        msgId,
        validationErrorMessage
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка валидации сообщения из SOFR QManager",
        QMNGR_READ_MSG_VALIDATION_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, topic)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CAUSE, validationErrorMessage)
            .build()
    );
  }

  /**
   * Логирование и аудит ошибки отправки в MQ загруженного из SOFR QManager сообщения.
   *
   * @param readMsg    контейнер с информацией о сообщении из SOFR QManager для отправки в MQ
   * @param errorCause причина ошибки
   */
  public void auditReadMsgToMqSendingError(@NonNull QmngrReadMsgDto readMsg,
                                           @NonNull String errorCause) {
    final String msgId = readMsg.getMsgId();
    log.error(
        "Error while sending loaded read msg with id = [{}] from SOFR QManager. Cause: {}.",
        msgId,
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка отправки сообщения",
        MQ_READ_MSG_SENDING_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(MSG_ID, msgId)
            .build()
    );
  }

  /**
   * Логирование и аудит успешной отправки в MQ загруженного из SOFR QManager сообщения.
   *
   * @param readMsg контейнер с информацией о сообщении из SOFR QManager для отправки в MQ
   */
  public void auditReadMsgToMqSendingSuccess(@NonNull QmngrReadMsgDto readMsg) {
    final String msgId = readMsg.getMsgId();
    log.info("Loaded read msg with id = [{}] sent successfully from SOFR QManager", msgId);
    auditEventSenderService.sendAuditEvent(
        "Сообщение успешно отправлено",
        MQ_READ_MSG_SENDING_SUCCESS,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(MSG_ID, msgId)
            .build()
    );
  }

  /**
   * Логирование и аудит ошибки обработки загруженного из SOFR QManager сообщения.
   *
   * @param readMsg    контейнер с информацией о сообщении из SOFR QManager для отправки в MQ
   * @param errorCause причина ошибки
   */
  public void auditReadMsgProcessingError(@NonNull QmngrReadMsgDto readMsg,
                                          @NonNull String errorCause) {
    log.error(
        "Error while processing loaded read msg = [{}] from SOFR QManager. Cause: {}.",
        readMsg.getMsgId(),
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка обработки сообщения из SOFR QManager",
        QMNGR_READ_MSG_PROCESSING_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, readMsg.getTopic())
            .withParam(MSG_ID, readMsg.getMsgId())
            .build()
    );
  }

}
