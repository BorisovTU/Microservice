package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit;

import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.ERROR;
import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.INFO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;

/**
 * Перечисление событий аудита по направлению обмена MQ -> SOFR QManager.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrLoadMsgAuditEvent implements AuditEvent {

  MQ_LOAD_MSG_RECEIVED(INFO, "Получено сообщение из Kafka для сохранения в SOFR QManager"),
  MQ_LOAD_MSG_VALIDATION_ERROR(ERROR, "Ошибка валидации сообщения из Kafka для сохранения в SOFR QManager"),
  MQ_LOAD_MSG_PROCESSING_ERROR(ERROR, "Ошибка обработки сообщения из Kafka для сохранения в SOFR QManager"),
  QMNGR_LOAD_MSG_PROC_CALL_ERROR(ERROR, "Ошибка вызова процедуры сохранения сообщения из Kafka в SOFR QManager"),
  QMNGR_LOAD_MSG_PROC_ERROR(ERROR, "Ошибка сохранения сообщения из Kafka в SOFR QManager"),
  QMNGR_LOAD_MSG_PROC_SUCCESS(INFO, "Сообщение из Kafka успешно сохранено в SOFR QManager");

  private final SeverityEnum severity;
  private final String message;

}
