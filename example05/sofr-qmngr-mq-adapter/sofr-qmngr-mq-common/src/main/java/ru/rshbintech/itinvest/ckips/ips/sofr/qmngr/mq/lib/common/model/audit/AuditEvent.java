package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit;

import org.springframework.lang.NonNull;
import ru.rshbintech.itinvest.ckips.audit.model.AuditRecord.SeverityEnum;

/**
 * Интерфейс для описания событий аудита.
 */
public interface AuditEvent {

  @NonNull
  SeverityEnum getSeverity();

  @NonNull
  String getMessage();

}
