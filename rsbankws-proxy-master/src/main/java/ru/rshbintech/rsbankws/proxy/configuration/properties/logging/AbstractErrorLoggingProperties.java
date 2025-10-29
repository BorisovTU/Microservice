package ru.rshbintech.rsbankws.proxy.configuration.properties.logging;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public abstract class AbstractErrorLoggingProperties {

    /**
     * Включено ли логирование при ошибках.
     */
    private boolean enabled = true;
    /**
     * Максимальная длина логируемой сущности при ошибках.
     */
    @NotNull
    @Min(500)
    @Max(10_000)
    private Integer maxLength = 10_000;

}
