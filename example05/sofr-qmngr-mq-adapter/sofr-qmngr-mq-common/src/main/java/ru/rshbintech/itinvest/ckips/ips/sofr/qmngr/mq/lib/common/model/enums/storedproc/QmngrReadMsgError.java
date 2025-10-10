package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.enums.storedproc;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Перечисление с внутренними ошибками адаптера при обработке результата вызова хранимой процедуры qmanager_read_msg.
 */
@Getter
@RequiredArgsConstructor
public enum QmngrReadMsgError {

  UNKNOWN_ERROR(1, "Хранимая процедура завершена с неизвестным статусом"),
  VALIDATION_ERROR(2, "Ошибка валидации результата вызова хранимой процедуры");

  private final int code;
  private final String description;

}
