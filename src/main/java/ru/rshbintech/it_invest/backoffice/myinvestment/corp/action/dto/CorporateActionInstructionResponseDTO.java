package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Schema(description = "Ответ об успешной обработке инструкции")
public class CorporateActionInstructionResponseDTO {
    @Schema(description = "Флаг успешности операции", example = "true")
    private boolean success;

    @Schema(description = "Сообщение", example = " ")
    private String message;

    public CorporateActionInstructionResponseDTO() {
        this.success = true;
        this.message = "Инструкция успешно принята";
    }
}
