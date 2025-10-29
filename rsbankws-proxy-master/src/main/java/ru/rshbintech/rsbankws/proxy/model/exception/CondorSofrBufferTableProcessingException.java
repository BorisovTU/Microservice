package ru.rshbintech.rsbankws.proxy.model.exception;

import lombok.Getter;
import org.springframework.lang.NonNull;

import java.util.Map;

@Getter
public class CondorSofrBufferTableProcessingException extends RuntimeException {

    private final String storedProcName;
    private final transient Map<String, Object> inParams;

    public CondorSofrBufferTableProcessingException(@NonNull String storedProcName,
                                                    @NonNull Map<String, Object> inParams,
                                                    @NonNull String message) {
        super(message);
        this.storedProcName = storedProcName;
        this.inParams = inParams;
    }

    public CondorSofrBufferTableProcessingException(@NonNull String storedProcName,
                                                    @NonNull Map<String, Object> inParams,
                                                    @NonNull String message,
                                                    @NonNull Throwable cause) {
        super(message, cause);
        this.storedProcName = storedProcName;
        this.inParams = inParams;
    }

}
