package ru.rshbintech.rsbankws.proxy.model.exception;

import lombok.Getter;
import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

@Getter
public class RSBankWSClientException extends RuntimeException {

    private final String soapEnvelopeRequest;
    private final HttpHeaders headers;

    public RSBankWSClientException(@Nullable String soapEnvelopeRequest,
                                   @Nullable HttpHeaders headers,
                                   @NonNull String message,
                                   @NonNull Throwable cause) {
        super(message, cause);
        this.soapEnvelopeRequest = soapEnvelopeRequest;
        this.headers = headers;
    }

    public RSBankWSClientException(@Nullable HttpHeaders headers,
                                   @NonNull String message,
                                   @NonNull Throwable cause) {
        this(null, headers, message, cause);
    }

}
