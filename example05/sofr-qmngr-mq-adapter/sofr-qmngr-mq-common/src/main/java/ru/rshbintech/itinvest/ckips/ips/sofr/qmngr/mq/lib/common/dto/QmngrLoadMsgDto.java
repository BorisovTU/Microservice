package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.dto;

import jakarta.validation.constraints.NotBlank;
import java.time.LocalDateTime;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/**
 * Контейнер с информацией о сообщении из MQ для сохранения в SOFR QManager.
 */
@Value
@Builder
public class QmngrLoadMsgDto {

  @NotBlank
  String topic;
  String msgId;
  LocalDateTime esbDt;
  @ToString.Exclude
  String headers;
  @NotBlank
  @ToString.Exclude
  String message;

}
