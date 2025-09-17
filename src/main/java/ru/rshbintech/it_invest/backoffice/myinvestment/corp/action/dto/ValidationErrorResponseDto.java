package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Schema(description = "Ответ об ошибке валидации")
public class ValidationErrorResponseDto {
    @Schema(description = "Список полей с ошибками", example = "clientId, isin, acct")
    private String error;

    @Schema(description = "Код ошибки", example = "MISSING_REQUIRED_FIELD")
    private String code;
}
