package ru.rshbintech.rsbankws.proxy.service.error.impl;

import brave.Tracer;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.configuration.properties.logging.ErrorLoggingProxyRequestBodyProperties;
import ru.rshbintech.rsbankws.proxy.configuration.properties.logging.ErrorLoggingProxyRequestHeadersProperties;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSProxyException;
import ru.rshbintech.rsbankws.proxy.service.error.ErrorService;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(
        {
                ErrorLoggingProxyRequestBodyProperties.class,
                ErrorLoggingProxyRequestHeadersProperties.class
        }
)
public class RSBankWSProxyErrorService implements ErrorService<RSBankWSProxyException> {

    private final Tracer tracer;
    private final ErrorLoggingProxyRequestBodyProperties errorLoggingProxyRequestBodyProperties;
    private final ErrorLoggingProxyRequestHeadersProperties errorLoggingProxyRequestHeadersProperties;

    @NonNull
    public String makeLogText(@NonNull RSBankWSProxyException rsBankWSProxyException) {
        final List<String> requestAttributes = new ArrayList<>(2);
        final HttpHeaders headers = rsBankWSProxyException.getHeaders();
        if (headers != null && !headers.isEmpty() && errorLoggingProxyRequestHeadersProperties.isEnabled()) {
            requestAttributes.add(
                    String.format(
                            "заголовки = [%s]",
                            StringUtils.abbreviate(
                                    Objects.toString(headers),
                                    errorLoggingProxyRequestHeadersProperties.getMaxLength()
                            )
                    )
            );
        }
        final String soapEnvelopeXmlAsString = rsBankWSProxyException.getSoapEnvelopeXmlAsString();
        if (StringUtils.isNotEmpty(soapEnvelopeXmlAsString) && errorLoggingProxyRequestBodyProperties.isEnabled()) {
            requestAttributes.add(
                    String.format(
                            "тело = [%s]",
                            StringUtils.abbreviate(
                                    soapEnvelopeXmlAsString,
                                    errorLoggingProxyRequestBodyProperties.getMaxLength()
                            )
                    )
            );
        }
        final String logText;
        final String message = rsBankWSProxyException.getMessage();
        if (CollectionUtils.isEmpty(requestAttributes)) {
            logText = message + ".";
        } else {
            logText = String.format("%s. Атрибуты запроса: %n%s.", message, String.join("\n", requestAttributes));
        }
        return logText;
    }

    @NonNull
    public String makeFaultText(@NonNull RSBankWSProxyException rsBankWSProxyException) {
        return String.format(
                "%s. Идентификатор ошибки: %s. Причина: ",
                rsBankWSProxyException.getMessage(),
                tracer.currentSpan().context().traceIdString()
        );
    }

}
