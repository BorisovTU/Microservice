package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.audit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEventParam;

/**
 * Общее перечисление параметров событий аудита.
 */
@Getter
@RequiredArgsConstructor
public enum BaseAuditEventParam implements AuditEventParam {

  TOPIC("topic"),
  MSG_ID("msg_id"),
  ESB_DT("esb_dt"),
  HEADERS("headers"),
  MESSAGE("message"),
  ERROR_CAUSE("error_cause"),
  PROC_NAME("proc_name"),
  ERROR_CODE("error_code"),
  ERROR_DESC("error_desc");

  private final String name;

}
