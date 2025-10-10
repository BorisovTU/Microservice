package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.to.qmngr.kafka.model.enums;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.audit.AuditEventParam;

/**
 * Перечисление параметров событий аудита Kafka.
 */
@Getter
@RequiredArgsConstructor
public enum KafkaAuditEventParam implements AuditEventParam {

  TOPIC("topic"),
  HEADERS("headers"),
  MSG_KEY("msg_key"),
  MSG_VALUE("msg_value");

  private final String name;

}
