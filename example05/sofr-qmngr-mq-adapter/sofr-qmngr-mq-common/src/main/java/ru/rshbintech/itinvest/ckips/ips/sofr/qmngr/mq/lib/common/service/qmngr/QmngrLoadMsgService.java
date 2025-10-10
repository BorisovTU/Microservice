package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.qmngr;

import static ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc.QmngrStoredProcErrorCode.SUCCESS;

import io.micrometer.core.annotation.Timed;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dao.QmngrDao;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto.QmngrLoadMsgDto;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.mdc.MdcAdapter;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.ValidationResult;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.MqType;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.storedproc.QmngrLoadMsgCall;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.service.audit.QmngrLoadMsgAuditService;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.validation.GenericValidator;

/**
 * Сервис обработки сообщений из MQ с последующим их сохранением в SOFR QManager.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class QmngrLoadMsgService {

  private final QmngrDao qmngrDao;
  private final MdcAdapter mdcAdapter;
  private final GenericValidator validator;
  private final QmngrLoadMsgAuditService loadMsgAuditService;

  /**
   * Метод производит обработку сообщения, полученного от MQ с последующим их сохранением в SOFR QManager.
   *
   * @param loadMsg сообщение, полученное от MQ для последующего сохранения в SOFR QManager.
   */
  @Timed(value = "rshbintech.sofr.qmngr.mq.load.msg.time", description = "QManager load msg time", histogram = true)
  public void process(@NonNull QmngrLoadMsgDto loadMsg, @NonNull MqType mqType) {
    try {
      mdcAdapter.putCorrelationId(loadMsg.getMsgId());
      final ValidationResult validationResult = validator.validate(loadMsg);
      if (validationResult.isValid()) {
        loadMsgAuditService.auditLoadMsgReceived(loadMsg, mqType);
        callLoadMsgStoredProc(loadMsg, mqType);
      } else {
        loadMsgAuditService.auditLoadMsgValidationError(loadMsg, mqType, validationResult.getErrorMsg());
      }
    } catch (Exception e) {
      loadMsgAuditService.auditLoadMsgProcessingError(loadMsg, mqType, ExceptionUtils.getStackTrace(e));
    } finally {
      mdcAdapter.clear();
    }
  }

  private void callLoadMsgStoredProc(@NonNull QmngrLoadMsgDto loadMsg, @NonNull MqType mqType) {
    final QmngrLoadMsgCall loadMsgCall = new QmngrLoadMsgCall(loadMsg);
    try {
      loadMsgAuditService.logLoadMsgCall(loadMsgCall);
      qmngrDao.callLoadMsg(loadMsgCall);
      final Integer errorCode = loadMsgCall.getErrorCode();
      if (errorCode == null) {
        loadMsgAuditService.auditLoadMsgCallUnknownStateCompletion(loadMsgCall, mqType);
      } else if (Objects.equals(errorCode, SUCCESS.getCode())) {
        loadMsgAuditService.auditLoadMsgCallSuccessCompletion(loadMsgCall, mqType);
      } else {
        loadMsgAuditService.auditLoadMsgCallErrorCompletion(loadMsgCall, mqType);
      }
    } catch (Exception e) {
      loadMsgAuditService.auditLoadMsgCallError(loadMsgCall, mqType, ExceptionUtils.getStackTrace(e));
    }
  }

}
