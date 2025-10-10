package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление с информацией о хранимых процедурах SOFR QManager.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrStoredProc {

  LOAD_MSG("qmanager_load_msg"),
  READ_MSG("qmanager_read_msg"),
  READ_MSG_ERROR("qmanager_read_msg_error"),
  GET_VALUES_METRICS("getValuesMetrics");

  private final String name;

  @Override
  public String toString() {
    return this.getName();
  }

}
