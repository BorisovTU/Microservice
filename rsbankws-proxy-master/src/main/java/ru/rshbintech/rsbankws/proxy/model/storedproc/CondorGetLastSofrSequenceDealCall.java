package ru.rshbintech.rsbankws.proxy.model.storedproc;

import lombok.Builder;
import lombok.Getter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.DEAL_CODE;
import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.ERROR_MESSAGE;
import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.REQUEST_ID;
import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.REQ_TYPE;
import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.SEQ_ID;
import static ru.rshbintech.rsbankws.proxy.model.enums.CondorGetLastSofrSequenceDealProcParam.SEQ_TYPE;

public class CondorGetLastSofrSequenceDealCall {

    @Getter
    private Map<String, Object> inParams;
    @SuppressWarnings("unused")
    private Map<String, Object> outParams;

    @Builder
    public CondorGetLastSofrSequenceDealCall(@NonNull String reqType,
                                             @Nullable String seqType,
                                             @NonNull String dealCode,
                                             @NonNull String requestId) {
        putInParam(REQ_TYPE, reqType);
        putInParam(SEQ_TYPE, seqType);
        putInParam(DEAL_CODE, dealCode);
        putInParam(REQUEST_ID, requestId);
    }

    private void putInParam(@NonNull CondorGetLastSofrSequenceDealProcParam param, @Nullable Object value) {
        if (this.inParams == null) {
            this.inParams = new HashMap<>();
        }
        this.inParams.put(param.getName(), value);
    }

    @Nullable
    public Integer getSeqId() {
        return getOutParam(SEQ_ID);
    }

    @Nullable
    public String getErrorMessage() {
        return getOutParam(ERROR_MESSAGE);
    }

    @Nullable
    private <T> T getOutParam(@NonNull CondorGetLastSofrSequenceDealProcParam param) {
        //noinspection unchecked
        return (T) Optional.ofNullable(this.outParams)
                .map(p -> p.get(param.getName()))
                .orElse(null);
    }

}
