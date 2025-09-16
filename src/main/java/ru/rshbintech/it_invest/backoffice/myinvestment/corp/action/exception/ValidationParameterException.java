package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Getter
@Setter
public class ValidationParameterException extends RuntimeException {
    private final String parameter;
    private final String description;
}
