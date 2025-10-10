package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление имен полей MDC.
 */
@Getter
@RequiredArgsConstructor
public enum MdcField {

  CORRELATION_ID("correlation_id");

  private final String name;

}
