package ru.rshbintech.rsbankws.proxy.model.exception;

import lombok.Getter;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Getter
public class RSBankWSProxyException extends RuntimeException {

    private final String soapEnvelopeXmlAsString;
    private final HttpHeaders headers;

    public RSBankWSProxyException(@Nullable String soapEnvelopeXmlAsString,
                                  @Nullable HttpHeaders headers,
                                  @NonNull String message,
                                  @NonNull Throwable cause) {
        super(message, cause);
        this.soapEnvelopeXmlAsString = soapEnvelopeXmlAsString;
        this.headers = headers;
    }

    public RSBankWSProxyException(@Nullable HttpHeaders headers,
                                  @NonNull String message,
                                  @NonNull Throwable cause) {
        this(null, headers, message, cause);
    }

}
