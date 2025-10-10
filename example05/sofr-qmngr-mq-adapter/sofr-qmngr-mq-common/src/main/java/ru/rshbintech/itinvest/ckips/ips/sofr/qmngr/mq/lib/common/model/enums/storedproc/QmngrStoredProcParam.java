package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление с параметрами вызова хранимых процедур SOFR QManager.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrStoredProcParam {

  TOPIC("topic"),
  MSG_ID("msgId"),
  ESB_DT("esbDt"),
  HEADERS("headers"),
  MESSAGE("message"),
  ERROR_CODE("errorCode"),
  ERROR_DESC("errorDesc"),
  WAIT_MSG("waitMsg"),
  OUT_JSON("outJson");

  private final String name;

}
