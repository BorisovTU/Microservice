package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit;

import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.ERROR;
import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.INFO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;

/**
 * Перечисление событий аудита для процесса установки ошибочного статуса обработки сообщения в SOFR QManager.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrReadMsgErrorAuditEvent implements AuditEvent {

  QMNGR_READ_MSG_ERROR_PROC_CALL_ERROR(
      ERROR,
      "Ошибка вызова процедуры установки ошибочного статуса обработки сообщения в SOFR QManager"
  ),
  QMNGR_READ_MSG_ERROR_PROC_SUCCESS(
      INFO,
      "Процедура установки ошибочного статуса обработки сообщения в SOFR QManager завершена успешно"
  );

  private final SeverityEnum severity;
  private final String message;

}
