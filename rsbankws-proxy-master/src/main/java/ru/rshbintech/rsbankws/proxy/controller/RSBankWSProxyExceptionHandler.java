package ru.rshbintech.rsbankws.proxy.controller;

import brave.Tracer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSClientException;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSProxyException;
import ru.rshbintech.rsbankws.proxy.service.HttpHeadersService;
import ru.rshbintech.rsbankws.proxy.service.error.ErrorService;
import ru.rshbintech.rsbankws.proxy.service.error.InternalErrorService;
import ru.rshbintech.rsbankws.proxy.service.error.impl.RSBankWSClientErrorService;
import ru.rshbintech.rsbankws.proxy.service.error.impl.RSBankWSProxyErrorService;
import ru.rshbintech.rsbankws.proxy.service.error.impl.UnknownErrorService;
import ru.rshbintech.rsbankws.proxy.service.xml.FaultAnswerXmlService;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.springframework.http.HttpStatus.OK;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.MSG_ERROR_PROXY;

@Slf4j
@RestControllerAdvice
@RequiredArgsConstructor
@SuppressWarnings("unused")
public class RSBankWSProxyExceptionHandler {

    private final Tracer tracer;
    private final HttpHeadersService httpHeadersService;
    private final UnknownErrorService unknownErrorService;
    private final FaultAnswerXmlService faultAnswerXmlService;
    private final RSBankWSProxyErrorService rsBankWSProxyErrorService;
    private final RSBankWSClientErrorService rsBankWSClientErrorService;
    private final List<InternalErrorService<? extends Throwable>> internalErrorServicesList;

    private Map<Class<? extends Throwable>, ErrorService<Throwable>> errorServicesMap;

    @PostConstruct
    @SuppressWarnings("unchecked")
    private void init() {
        errorServicesMap = internalErrorServicesList.stream()
                .collect(
                        Collectors.toMap(
                                InternalErrorService::getErrorType,
                                internalErrorService -> (InternalErrorService<Throwable>) internalErrorService
                        )
                );
    }

    @NonNull
    @ExceptionHandler(RSBankWSProxyException.class)
    public ResponseEntity<String> handleRSBankWSProxyException(@NonNull RSBankWSProxyException rsBankWSProxyException) {
        final Throwable cause = rsBankWSProxyException.getCause();
        final ErrorService<Throwable> errorService = errorServicesMap.getOrDefault(
                rsBankWSProxyException.getCause().getClass(),
                unknownErrorService
        );
        log.error(rsBankWSProxyErrorService.makeLogText(rsBankWSProxyException));
        log.error(errorService.makeLogText(cause));
        return new ResponseEntity<>(
                makeFaultAnswerXmlAsString(
                        rsBankWSProxyErrorService.makeFaultText(rsBankWSProxyException)
                                + errorService.makeFaultText(cause)
                ),
                httpHeadersService.prepareHttpHeadersForAnswer(),
                OK
        );
    }

    @NonNull
    @ExceptionHandler(RSBankWSClientException.class)
    public ResponseEntity<String> handleRSBankWSClientException(
            @NonNull RSBankWSClientException rsBankWSClientException) {
        log.error(MSG_ERROR_PROXY);
        log.error(rsBankWSClientErrorService.makeLogText(rsBankWSClientException));
        return new ResponseEntity<>(
                makeFaultAnswerXmlAsString(rsBankWSClientErrorService.makeFaultText(rsBankWSClientException)),
                httpHeadersService.prepareHttpHeadersForAnswer(),
                OK
        );
    }

    @NonNull
    @ExceptionHandler(Throwable.class)
    public ResponseEntity<String> handleUnknownError(@NonNull Throwable throwable) {
        log.error(MSG_ERROR_PROXY);
        log.error(unknownErrorService.makeLogText(throwable));
        return new ResponseEntity<>(
                makeFaultAnswerXmlAsString(
                        String.format(
                                "%s. Идентификатор ошибки: %s. Причина: %s",
                                MSG_ERROR_PROXY,
                                tracer.currentSpan().context().traceIdString(),
                                unknownErrorService.makeFaultText(throwable)
                        )
                ),
                httpHeadersService.prepareHttpHeadersForAnswer(),
                OK
        );
    }

    private String makeFaultAnswerXmlAsString(@NonNull String faultString) {
        return faultAnswerXmlService.makeFaultAnswerXmlAsString(5, -1, faultString);
    }

}
