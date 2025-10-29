package ru.rshbintech.rsbankws.proxy.service.error.impl;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.model.exception.CondorSofrBufferTableProcessingException;
import ru.rshbintech.rsbankws.proxy.service.error.InternalErrorService;

@Service
@SuppressWarnings("unused")
public class CondorSofrBufferTableProcessingErrorService
        implements InternalErrorService<CondorSofrBufferTableProcessingException> {

    @NonNull
    @Override
    public Class<CondorSofrBufferTableProcessingException> getErrorType() {
        return CondorSofrBufferTableProcessingException.class;
    }

    @NonNull
    @Override
    public String makeLogText(
            @NonNull CondorSofrBufferTableProcessingException condorSofrBufferTableProcessingException) {
        String logText = makeFaultText(condorSofrBufferTableProcessingException);
        final Throwable cause = condorSofrBufferTableProcessingException.getCause();
        if (cause != null) {
            logText += String.format("%nПричина:%n%s", ExceptionUtils.getStackTrace(cause));
        }
        return logText;
    }

}
