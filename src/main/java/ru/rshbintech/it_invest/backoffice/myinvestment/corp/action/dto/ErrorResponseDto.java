package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@Schema(description = "Ответ об ошибке")
public class ErrorResponseDto {
    @Schema(description = "Описание ошибки", example = " ")
    private String error;

    @Schema(description = "Код ошибки", example = "INTERNAL_SERVER_ERROR")
    private String code;
}
