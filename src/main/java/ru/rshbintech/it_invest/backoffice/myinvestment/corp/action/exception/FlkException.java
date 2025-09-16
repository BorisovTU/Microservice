package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class FlkException extends RuntimeException {
    private final String code;
    private final String message;
}
