package ru.rshbintech.rsbankws.proxy.model.exception;

import lombok.Getter;
import org.springframework.lang.NonNull;
import ru.rshbintech.rsbankws.proxy.model.XmlInfo;
import ru.rshbintech.rsbankws.proxy.model.enums.XmlType;

@Getter
public class XmlProcessingException extends RuntimeException {

    private final String xmlAsString;
    private final XmlType xmlType;

    public XmlProcessingException(@NonNull String xmlAsString, @NonNull XmlType xmlType, @NonNull String message) {
        super(message);
        this.xmlAsString = xmlAsString;
        this.xmlType = xmlType;
    }

    public XmlProcessingException(@NonNull String xmlAsString,
                                  @NonNull XmlType xmlType,
                                  @NonNull String message,
                                  @NonNull Throwable cause) {
        super(message, cause);
        this.xmlAsString = xmlAsString;
        this.xmlType = xmlType;
    }

    public XmlProcessingException(@NonNull XmlInfo xmlInfo, @NonNull String message) {
        super(message);
        this.xmlAsString = xmlInfo.getXmlAsString();
        this.xmlType = xmlInfo.getXmlType();
    }

    public XmlProcessingException(@NonNull XmlInfo xmlInfo, @NonNull String message, @NonNull Throwable cause) {
        super(message, cause);
        this.xmlAsString = xmlInfo.getXmlAsString();
        this.xmlType = xmlInfo.getXmlType();
    }

}
