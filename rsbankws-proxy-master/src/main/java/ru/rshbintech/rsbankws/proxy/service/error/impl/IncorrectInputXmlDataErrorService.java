package ru.rshbintech.rsbankws.proxy.service.error.impl;

import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.model.exception.IncorrectInputXmlDataException;
import ru.rshbintech.rsbankws.proxy.service.error.InternalErrorService;

@Service
@SuppressWarnings("unused")
public class IncorrectInputXmlDataErrorService implements InternalErrorService<IncorrectInputXmlDataException> {

    @NonNull
    @Override
    public Class<IncorrectInputXmlDataException> getErrorType() {
        return IncorrectInputXmlDataException.class;
    }

    @NonNull
    @Override
    public String makeLogText(@NonNull IncorrectInputXmlDataException incorrectInputXmlDataException) {
        return makeFaultText(incorrectInputXmlDataException);
    }

}
