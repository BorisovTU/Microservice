package ru.rshbintech.rsbankws.proxy.service.error;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.configuration.properties.logging.ErrorLoggingRSBankWSRequestBodyProperties;
import ru.rshbintech.rsbankws.proxy.configuration.properties.logging.ErrorLoggingRSBankWSRequestHeadersProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(
        {
                ErrorLoggingRSBankWSRequestBodyProperties.class,
                ErrorLoggingRSBankWSRequestHeadersProperties.class
        }
)
public class RSBankWSClientErrorLoggingService {

    private final ErrorLoggingRSBankWSRequestBodyProperties errorLoggingRSBankWSRequestBodyProperties;
    private final ErrorLoggingRSBankWSRequestHeadersProperties errorLoggingRSBankWSRequestHeadersProperties;

    @NonNull
    public String makeLogText(@NonNull String soapEnvelopeXmlAsString,
                              @Nullable HttpHeaders headers,
                              @NonNull String logMessage,
                              @Nullable Throwable cause) {
        final List<String> requestAttributes = new ArrayList<>(2);
        if (headers != null && !headers.isEmpty() && errorLoggingRSBankWSRequestHeadersProperties.isEnabled()) {
            requestAttributes.add(
                    String.format(
                            "заголовки = [%s]",
                            StringUtils.abbreviate(
                                    Objects.toString(headers),
                                    errorLoggingRSBankWSRequestHeadersProperties.getMaxLength()
                            )
                    )
            );
        }
        if (StringUtils.isNotEmpty(soapEnvelopeXmlAsString) && errorLoggingRSBankWSRequestBodyProperties.isEnabled()) {
            requestAttributes.add(
                    String.format(
                            "тело = [%s]",
                            StringUtils.abbreviate(
                                    soapEnvelopeXmlAsString,
                                    errorLoggingRSBankWSRequestBodyProperties.getMaxLength()
                            )
                    )
            );
        }
        String logText;
        if (CollectionUtils.isEmpty(requestAttributes)) {
            logText = logMessage + ".";
        } else {
            logText = String.format("%s. Атрибуты запроса: %n%s.", logMessage, String.join("\n", requestAttributes));
        }
        if (cause != null) {
            logText += String.format("%nПричина:%n%s", ExceptionUtils.getStackTrace(cause));
        }
        return logText;
    }

}
