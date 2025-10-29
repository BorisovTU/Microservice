package ru.rshbintech.rsbankws.proxy.model.exception;

import lombok.Getter;
import org.springframework.lang.NonNull;

@Getter
public class IncorrectInputXmlDataException extends RuntimeException {

    public IncorrectInputXmlDataException(@NonNull String message) {
        super(message);
    }

}
