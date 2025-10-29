package ru.rshbintech.rsbankws.proxy.service.error;

import org.springframework.lang.NonNull;

public interface InternalErrorService<T extends Throwable> extends ErrorService<T> {

    @NonNull
    Class<T> getErrorType();

    @NonNull
    @Override
    default String makeFaultText(@NonNull T throwable) {
        return String.format("%s.", throwable.getMessage());
    }

}
