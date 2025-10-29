package ru.rshbintech.rsbankws.proxy.service.error.impl;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.service.error.ErrorService;

@Service
@SuppressWarnings("unused")
public class UnknownErrorService implements ErrorService<Throwable> {

    private static final String UNKNOWN_ERROR_TEXT = "Неизвестная ошибка";

    @NonNull
    @Override
    public String makeLogText(@NonNull Throwable throwable) {
        return String.format("%s. %nПричина:%n%s", UNKNOWN_ERROR_TEXT, ExceptionUtils.getStackTrace(throwable));
    }

    @NonNull
    @Override
    public String makeFaultText(@NonNull Throwable throwable) {
        return String.format("%s.", UNKNOWN_ERROR_TEXT);
    }

}
