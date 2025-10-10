package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model;

import lombok.Builder;
import lombok.Value;

/**
 * Контейнер с результатом валидации.
 */
@Value
@Builder
public class ValidationResult {

  private static final ValidationResult OK = ValidationResult.builder()
      .valid(true)
      .build();

  public static ValidationResult ok() {
    return OK;
  }

  boolean valid;
  String errorMsg;

}
