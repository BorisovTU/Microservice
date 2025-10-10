package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление с возможными статусами вызова хранимых процедур SOFR QManager.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrStoredProcErrorCode {

  SUCCESS(0),
  NO_MESSAGES(25228);

  private final int code;

}
