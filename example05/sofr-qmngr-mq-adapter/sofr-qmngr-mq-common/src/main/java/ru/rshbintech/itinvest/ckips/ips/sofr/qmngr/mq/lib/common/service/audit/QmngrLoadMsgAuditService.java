package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CAUSE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_CODE;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ERROR_DESC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.ESB_DT;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.MSG_ID;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.PROC_NAME;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.BaseAuditEventParam.TOPIC;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_PROCESSING_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_RECEIVED;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.MQ_LOAD_MSG_VALIDATION_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_CALL_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_ERROR;
import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit.QmngrLoadMsgAuditEvent.QMNGR_LOAD_MSG_PROC_SUCCESS;

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProc;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.logging.MessageLoggingService;

/**
 * Сервис аудита процесса получения сообщений из MQ с последующим их сохранением в SOFR QManager.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrLoadMsgAuditService {

  private final MessageLoggingService messageLoggingService;
  private final AuditEventSenderService auditEventSenderService;
  private final AuditEventParamsBuilderFactory auditEventParamsBuilderFactory;

  /**
   * Логирование и аудит успешного получения сообщения из MQ.
   *
   * @param loadMsg контейнер с информацией о сообщении из MQ для сохранения в SOFR QManager
   * @param mqType  тип MQ
   */
  public void auditLoadMsgReceived(@NonNull QmngrLoadMsgDto loadMsg, @NonNull MqType mqType) {
    final String headers = loadMsg.getHeaders();
    final String message = loadMsg.getMessage();
    log.info(
        "Received load msg = [{}]{} from {}",
        loadMsg,
        messageLoggingService.getLogMessageText(headers, message),
        mqType
    );
    auditEventSenderService.sendAuditEvent(
        String.format("Получено сообщение из %s", mqType),
        MQ_LOAD_MSG_RECEIVED,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, loadMsg.getTopic())
            .withParam(MSG_ID, loadMsg.getMsgId())
            .withParam(ESB_DT, loadMsg.getEsbDt())
            .withHeadersParam(headers)
            .withMessageParam(message)
            .build()
    );
  }

  /**
   * Логирование и аудит ошибки валидации сообщения из MQ.
   *
   * @param loadMsg                контейнер с информацией о сообщении из MQ для сохранения в SOFR QManager
   * @param mqType                 тип MQ
   * @param validationErrorMessage причина ошибки
   */
  public void auditLoadMsgValidationError(@NonNull QmngrLoadMsgDto loadMsg,
                                          @NonNull MqType mqType,
                                          @NonNull String validationErrorMessage) {
    final String headers = loadMsg.getHeaders();
    final String message = loadMsg.getMessage();
    log.error(
        "Received load msg = [{}]{} from {} has validation errors:\n{}",
        loadMsg,
        messageLoggingService.getLogMessageText(headers, message),
        mqType,
        validationErrorMessage
    );
    auditEventSenderService.sendAuditEvent(
        String.format("Ошибка валидации сообщения из %s", mqType),
        MQ_LOAD_MSG_VALIDATION_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, loadMsg.getTopic())
            .withParam(MSG_ID, loadMsg.getMsgId())
            .withParam(ESB_DT, loadMsg.getEsbDt())
            .withHeadersParam(headers)
            .withMessageParam(message)
            .withParam(ERROR_CAUSE, validationErrorMessage)
            .build()
    );
  }

  /**
   * Логирование и аудит ошибки обработки сообщения из MQ.
   *
   * @param loadMsg    контейнер с информацией о сообщении из MQ для сохранения в SOFR QManager
   * @param mqType     тип MQ
   * @param errorCause причина ошибки
   */
  public void auditLoadMsgProcessingError(@NonNull QmngrLoadMsgDto loadMsg,
                                          @NonNull MqType mqType,
                                          @NonNull String errorCause) {
    final String headers = loadMsg.getHeaders();
    final String message = loadMsg.getMessage();
    log.error(
        "Error while processing received load msg = [{}]{} from {}. Cause: {}.",
        loadMsg,
        messageLoggingService.getLogMessageText(headers, message),
        mqType,
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        String.format("Ошибка обработки сообщения из %s", mqType),
        MQ_LOAD_MSG_PROCESSING_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(TOPIC, loadMsg.getTopic())
            .withParam(MSG_ID, loadMsg.getMsgId())
            .withParam(ESB_DT, loadMsg.getEsbDt())
            .withHeadersParam(headers)
            .withMessageParam(message)
            .build()
    );
  }

  /**
   * Логирование вызова хранимой процедуры qmanager_load_msg.
   *
   * @param loadMsgCall контейнер с параметрами для вызова хранимой процедуры qmanager_load_msg
   */
  public void logLoadMsgCall(@NonNull QmngrLoadMsgCall loadMsgCall) {
    log.info(
        "Calling stored procedure = [{}] for message with id = [{}]",
        loadMsgCall.getQmngrStoredProc(),
        loadMsgCall.getMsgId()
    );
  }

  /**
   * Логирование и аудит ошибки вызова хранимой процедуры qmanager_load_msg.
   *
   * @param loadMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_load_msg
   * @param mqType      тип MQ
   * @param errorCause  причина ошибки
   */
  public void auditLoadMsgCallError(@NonNull QmngrLoadMsgCall loadMsgCall,
                                    @NonNull MqType mqType,
                                    @NonNull String errorCause) {
    final QmngrStoredProc storedProc = loadMsgCall.getQmngrStoredProc();
    final String msgId = loadMsgCall.getMsgId();
    log.error(
        "Error while call stored procedure = [{}] for message with id = [{}]. Cause: {}.",
        storedProc,
        msgId,
        errorCause
    );
    auditEventSenderService.sendAuditEvent(
        "Ошибка вызова хранимой процедуры",
        QMNGR_LOAD_MSG_PROC_CALL_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .build()
    );
  }

  /**
   * Логирование и аудит результата вызова хранимой процедуры qmanager_load_msg с неизвестным статусом.
   *
   * @param loadMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_load_msg
   * @param mqType      тип MQ
   */
  public void auditLoadMsgCallUnknownStateCompletion(@NonNull QmngrLoadMsgCall loadMsgCall,
                                                     @NonNull MqType mqType) {
    final QmngrStoredProc storedProc = loadMsgCall.getQmngrStoredProc();
    final String msgId = loadMsgCall.getMsgId();
    final String errorCode = Objects.toString(loadMsgCall.getErrorCode());
    final String errorDesc = loadMsgCall.getErrorDesc();
    log.error(
        "Stored procedure = [{}] for message with id = [{}] completed with unknown state = [{}{}]",
        storedProc,
        msgId,
        errorCode,
        StringUtils.isNotBlank(errorDesc) ? ": " + errorDesc : EMPTY
    );
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена с неизвестным статусом",
        QMNGR_LOAD_MSG_PROC_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, errorCode)
            .withParam(ERROR_DESC, errorDesc)
            .build()
    );
  }

  /**
   * Логирование и аудит успешного результата вызова хранимой процедуры qmanager_load_msg.
   *
   * @param loadMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_load_msg
   * @param mqType      тип MQ
   */
  public void auditLoadMsgCallSuccessCompletion(@NonNull QmngrLoadMsgCall loadMsgCall,
                                                @NonNull MqType mqType) {
    final QmngrStoredProc storedProc = loadMsgCall.getQmngrStoredProc();
    final String msgId = loadMsgCall.getMsgId();
    log.info("Stored procedure = [{}] for message with id = [{}] completed successfully", storedProc, msgId);
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена успешно",
        QMNGR_LOAD_MSG_PROC_SUCCESS,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .build()
    );
  }

  /**
   * Логирование и аудит результата вызова хранимой процедуры qmanager_load_msg с ошибочным статусом.
   *
   * @param loadMsgCall контейнер с параметрами вызова хранимой процедуры qmanager_load_msg
   * @param mqType      тип MQ
   */
  public void auditLoadMsgCallErrorCompletion(@NonNull QmngrLoadMsgCall loadMsgCall,
                                              @NonNull MqType mqType) {
    final QmngrStoredProc storedProc = loadMsgCall.getQmngrStoredProc();
    final String msgId = loadMsgCall.getMsgId();
    final Integer errorCode = loadMsgCall.getErrorCode();
    final String errorDesc = loadMsgCall.getErrorDesc();
    log.error(
        "Stored procedure = [{}] for message with id = [{}] completed with error state = [{}{}]",
        storedProc,
        msgId,
        errorCode,
        StringUtils.isNotBlank(errorDesc) ? ": " + errorDesc : EMPTY
    );
    auditEventSenderService.sendAuditEvent(
        "Хранимая процедура завершена с ошибкой",
        QMNGR_LOAD_MSG_PROC_ERROR,
        () -> auditEventParamsBuilderFactory.createAuditEventParamsBuilder()
            .withParam(PROC_NAME, storedProc)
            .withParam(MSG_ID, msgId)
            .withParam(ERROR_CODE, errorCode)
            .withParam(ERROR_DESC, errorDesc)
            .build()
    );
  }

}
