package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление с типами MQ.
 */
@Getter
@RequiredArgsConstructor
public enum MqType {

  KAFKA("Kafka");

  private final String name;

  @Override
  public String toString() {
    return this.getName();
  }

}
