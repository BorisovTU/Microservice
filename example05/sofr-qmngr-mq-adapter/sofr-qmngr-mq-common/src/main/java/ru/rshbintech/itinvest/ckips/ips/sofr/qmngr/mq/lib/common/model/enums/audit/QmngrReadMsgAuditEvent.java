package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit;

import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.ERROR;
import static ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum.INFO;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEvent;

/**
 * Перечисление событий аудита по направлению обмена SOFR QManager -> MQ.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrReadMsgAuditEvent implements AuditEvent {

  QMNGR_READ_MSG_PROC_CALL_ERROR(
      ERROR,
      "Ошибка вызова процедуры получения сообщения из SOFR QManager для отправки в Kafka"
  ),
  QMNGR_READ_MSG_PROC_ERROR(ERROR, "Ошибка получения сообщения из SOFR QManager для отправки в Kafka"),
  QMNGR_READ_MSG_RECEIVED(INFO, "Получено сообщение из SOFR QManager для отправки в Kafka"),
  QMNGR_READ_MSG_VALIDATION_ERROR(ERROR, "Ошибка валидации сообщения из SOFR QManager для отправки в Kafka"),
  QMNGR_READ_MSG_PROCESSING_ERROR(ERROR, "Ошибка обработки сообщения из SOFR QManager для отправки в Kafka"),
  MQ_READ_MSG_SENDING_ERROR(ERROR, "Ошибка отправки сообщения из SOFR QManager в Kafka"),
  MQ_READ_MSG_SENDING_SUCCESS(INFO, "Сообщение из SOFR QManager успешно отправлено в Kafka");

  private final SeverityEnum severity;
  private final String message;

}
