package ru.rshbintech.rsbankws.proxy.service.error.impl;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.configuration.properties.logging.ErrorLoggingProcessingXmlProperties;
import ru.rshbintech.rsbankws.proxy.model.exception.XmlProcessingException;
import ru.rshbintech.rsbankws.proxy.service.error.InternalErrorService;

@Service
@RequiredArgsConstructor
@SuppressWarnings("unused")
@EnableConfigurationProperties(ErrorLoggingProcessingXmlProperties.class)
public class XmlProcessingErrorService implements InternalErrorService<XmlProcessingException> {

    private final ErrorLoggingProcessingXmlProperties errorLoggingProcessingXmlProperties;

    @NonNull
    @Override
    public Class<XmlProcessingException> getErrorType() {
        return XmlProcessingException.class;
    }

    @NonNull
    @Override
    public String makeLogText(@NonNull XmlProcessingException xmlProcessingException) {
        String logText = String.format(
                "%s.%nТип XML = [%s].",
                xmlProcessingException.getMessage(),
                xmlProcessingException.getXmlType().getName()
        );
        final String xmlAsString = xmlProcessingException.getXmlAsString();
        if (StringUtils.isNotEmpty(xmlAsString) && errorLoggingProcessingXmlProperties.isEnabled()) {
            logText += String.format(
                    "%nТело XML = [%s].",
                    StringUtils.abbreviate(xmlAsString, errorLoggingProcessingXmlProperties.getMaxLength())
            );
        }
        final Throwable cause = xmlProcessingException.getCause();
        if (cause != null) {
            logText += String.format("%nПричина:%n%s", ExceptionUtils.getStackTrace(cause));
        }
        return logText;
    }

}
