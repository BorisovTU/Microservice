package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto;

import jakarta.validation.constraints.NotBlank;
import java.sql.Clob;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/**
 * Контейнер с информацией о сообщении из SOFR QManager для отправки в MQ.
 */
@Value
@Builder
public class QmngrReadMsgDto {

  @NotBlank
  String topic;
  String msgId;
  @ToString.Exclude
  String headers;
  @ToString.Exclude
  Clob message;

}
