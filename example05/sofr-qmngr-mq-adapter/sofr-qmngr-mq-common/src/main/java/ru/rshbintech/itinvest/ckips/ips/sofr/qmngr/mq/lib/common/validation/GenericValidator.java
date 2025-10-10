package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.validation;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.model.ValidationResult;

/**
 * Сервис проверки объектов с использованием jakarta validation api.
 */
@Component
@RequiredArgsConstructor
public class GenericValidator {

  private final Validator validator;

  /**
   * Метод производит проверку переданного объекта.
   *
   * @param valueToValidation проверяемое значение
   * @param <T>               тип проверяемого значения
   * @return результат проверки
   */
  @NonNull
  public <T> ValidationResult validate(@NonNull T valueToValidation) {
    final Set<ConstraintViolation<T>> validationErrors = validator.validate(valueToValidation);
    final ValidationResult validationResult;
    if (CollectionUtils.isEmpty(validationErrors)) {
      validationResult = ValidationResult.ok();
    } else {
      final String errorMsg = validationErrors.stream()
          .map(error -> String.format("Field [%s] %s", error.getPropertyPath(), error.getMessage()))
          .collect(Collectors.joining("\n"));
      validationResult = ValidationResult.builder()
          .valid(false)
          .errorMsg(errorMsg)
          .build();
    }
    return validationResult;
  }

}
