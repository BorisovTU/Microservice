package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.List;

@Data
@NoArgsConstructor
public class CorporateActionInstructionDTO {
    @NotNull(message = "CorporateActionInstruction is required")
    @Valid
    @JsonProperty("CorporateActionInstruction")
    @Schema(description = "Информация об инструкции")
    private CorporateActionInstruction corporateActionInstruction;

    @Data
    @NoArgsConstructor
    @Schema(description = "Информация об инструкции")
    public static class CorporateActionInstruction {
        @NotEmpty(message = "Как минимум 1 clientId должен быть заполнен")
        @Valid
        private List<ClientId> clientId;

        @NotBlank(message = "Обязательный параметр: CorpActnEvtId не указан")
        @Size(max = 50, message = "Кол-во символов поля CorpActnEvtId не должно превышать 50")
        private String corpActnEvtId;

        @NotBlank(message = "Обязательный параметр: InstrDt не указан")
        @Pattern(regexp = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$",
                message = "Поле InstrDt не соответствует формату YYYY-MM-DDThh:mm:ssZ")
        private String instrDt;

        @NotBlank(message = "Обязательный параметр: InstrNmb не указан")
        @Size(max = 50, message = "Кол-во символов поля InstrNmb  не должно превышать 50")
        private String instrNmb;

        @Size(min = 12, max = 12, message = "ISIN должен быть ровно из 12 символов")
        private String isin;

        @NotBlank(message = "Обязательный параметр: Acct не указан")
        //@Pattern(regexp = "^Д-Т-\\d{6}$", message = "Acct must be in format Д-Т-XXXXXX")
        private String acct;

        @NotEmpty(message = "Как минимум 1 CorpActnOptnDtls должен быть заполнен")
        @Valid
        private List<CorpActnOptnDtls> corpActnOptnDtls;
    }

    @Data
    @NoArgsConstructor
    public static class ClientId {
        @NotBlank(message = "Обязательный параметр: objectId не указан")
        @Pattern(regexp = "^\\d{12}$", message = "objectId must be exactly 12 digits")
        private String objectId;

        @NotBlank(message = "Обязательный параметр: systemId не указан")
        @Pattern(regexp = "^CFT$", message = "systemId must be 'CFT'")
        private String systemId;
    }

    @Data
    @NoArgsConstructor
    public static class CorpActnOptnDtls {
        @NotBlank(message = "Обязательный параметр: OptnNb не указан")
        @Pattern(regexp = "^\\d{3}$", message = "OptnNb должен состоять из 3 символов")
        private String optnNb;

        @NotBlank(message = "Обязательный параметр: OptnTp не указан")
        @Pattern(regexp = "^(CASH|NOAP)$", message = "OptnTp возможные значения: CASH или NOAP")
        private String optnTp;

        @DecimalMin(value = "0.0", inclusive = false, message = "PricVal должен быть больше 0")
        @Digits(integer = 15, fraction = 4, message = "PricVal целая часть не должна превышать 15 символов, а дробная часть не должна превышать 4 символов")
        private BigDecimal pricVal;

        @Pattern(regexp = "^\\d{3}$", message = "PricValCcy должен состоять из 3 символов")
        private String pricValCcy;

        @NotNull(message = "Обязательный параметр: Bal не указан")
        @Min(value = 1, message = "Bal must be at least 1")
        private Integer bal;
    }
}
