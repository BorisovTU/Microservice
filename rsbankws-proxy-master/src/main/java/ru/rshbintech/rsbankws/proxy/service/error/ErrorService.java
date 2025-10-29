package ru.rshbintech.rsbankws.proxy.service.error;

import org.springframework.lang.NonNull;

public interface ErrorService<T extends Throwable> {

    @NonNull
    String makeLogText(@NonNull T throwable);

    @NonNull
    String makeFaultText(@NonNull T throwable);

}
