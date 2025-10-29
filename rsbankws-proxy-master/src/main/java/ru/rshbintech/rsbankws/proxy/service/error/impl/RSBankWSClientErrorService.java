package ru.rshbintech.rsbankws.proxy.service.error.impl;

import brave.Tracer;
import lombok.RequiredArgsConstructor;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSClientException;
import ru.rshbintech.rsbankws.proxy.service.error.ErrorService;
import ru.rshbintech.rsbankws.proxy.service.error.RSBankWSClientErrorLoggingService;

import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.MSG_ERROR_PROXY;

@Service
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class RSBankWSClientErrorService implements ErrorService<RSBankWSClientException> {

    private final Tracer tracer;
    private final RSBankWSClientErrorLoggingService rsBankWSClientErrorLoggingService;

    @NonNull
    @Override
    public String makeLogText(@NonNull RSBankWSClientException rsBankWSClientException) {
        return rsBankWSClientErrorLoggingService.makeLogText(
                rsBankWSClientException.getSoapEnvelopeRequest(),
                rsBankWSClientException.getHeaders(),
                rsBankWSClientException.getMessage(),
                rsBankWSClientException.getCause()
        );
    }

    @NonNull
    public String makeFaultText(@NonNull RSBankWSClientException rsBankWSClientException) {
        return String.format(
                "%s. Идентификатор ошибки: %s. Причина: %s",
                MSG_ERROR_PROXY,
                tracer.currentSpan().context().traceIdString(),
                rsBankWSClientException.getMessage()
        );
    }

}
