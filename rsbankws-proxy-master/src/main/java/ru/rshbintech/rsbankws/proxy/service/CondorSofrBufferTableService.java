package ru.rshbintech.rsbankws.proxy.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerProperties;
import ru.rshbintech.rsbankws.proxy.dao.CondorSofrBufferTableDao;
import ru.rshbintech.rsbankws.proxy.model.ProcessDealsInfo;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.model.enums.ActionType;
import ru.rshbintech.rsbankws.proxy.model.enums.SeqType;
import ru.rshbintech.rsbankws.proxy.model.exception.CondorSofrBufferTableProcessingException;
import ru.rshbintech.rsbankws.proxy.model.storedproc.CondorGetLastSofrSequenceDealCall;

import java.util.Objects;

@Slf4j
@Service
@RequiredArgsConstructor
public class CondorSofrBufferTableService {

    private final FastAnswerProperties fastAnswerProperties;
    private final CondorSofrBufferTableDao condorSofrBufferTableDao;

    /**
     * В отличие от метода callCondorGetLastSofrSequenceDealForFastAnswer, здесь при ошибках исключения выбрасываться
     * не будут, так как ошибки в мониторинге никак не должны влиять на процесс проксирования запроса.
     */
    public void callCondorGetLastSofrSequenceDealForMonitoring(@NonNull ProxyRequestInfo proxyRequestInfo) {
        final CondorGetLastSofrSequenceDealCall condorGetLastSofrSequenceDealCall =
                createCondorGetLastSofrSequenceDealCall(proxyRequestInfo);
        try {
            condorSofrBufferTableDao.callCondorGetLastSofrSequenceDeal(condorGetLastSofrSequenceDealCall);
        } catch (Exception e) {
            log.error("Ошибка вызова хранимой процедуры. Причина: {}.", ExceptionUtils.getStackTrace(e));
        }
        final String errorMessage = condorGetLastSofrSequenceDealCall.getErrorMessage();
        if (StringUtils.isNotEmpty(errorMessage)) {
            log.error("Хранимая процедура завершена с ошибкой: {}.", errorMessage);
        }
    }

    @NonNull
    public String callCondorGetLastSofrSequenceDealForFastAnswer(@NonNull ProxyRequestInfo proxyRequestInfo) {
        final CondorGetLastSofrSequenceDealCall condorGetLastSofrSequenceDealCall =
                createCondorGetLastSofrSequenceDealCall(proxyRequestInfo);
        try {
            condorSofrBufferTableDao.callCondorGetLastSofrSequenceDeal(condorGetLastSofrSequenceDealCall);
        } catch (Exception e) {
            throw new CondorSofrBufferTableProcessingException(
                    fastAnswerProperties.getCondorSofrBufferTableInsertStoredProcName(),
                    condorGetLastSofrSequenceDealCall.getInParams(),
                    "Ошибка вызова хранимой процедуры",
                    e
            );
        }
        final String errorMessage = condorGetLastSofrSequenceDealCall.getErrorMessage();
        if (StringUtils.isNotEmpty(errorMessage)) {
            throw new CondorSofrBufferTableProcessingException(
                    fastAnswerProperties.getCondorSofrBufferTableInsertStoredProcName(),
                    condorGetLastSofrSequenceDealCall.getInParams(),
                    String.format("Хранимая процедура завершена с ошибкой: %s", errorMessage)
            );
        }
        final Integer seqId = condorGetLastSofrSequenceDealCall.getSeqId();
        if (seqId == null) {
            throw new CondorSofrBufferTableProcessingException(
                    fastAnswerProperties.getCondorSofrBufferTableInsertStoredProcName(),
                    condorGetLastSofrSequenceDealCall.getInParams(),
                    "Хранимая процедура не вернула сгенерированный последовательностью идентификатор"
            );
        }
        return Objects.toString(seqId);
    }

    private CondorGetLastSofrSequenceDealCall createCondorGetLastSofrSequenceDealCall(
            @NonNull ProxyRequestInfo proxyRequestInfo) {
        final ProcessDealsInfo processDealsInfo = proxyRequestInfo.getProcessDealsInfo();
        final SeqType seqType = processDealsInfo.getSeqType();
        final ActionType actionType = processDealsInfo.getActionType();
        return CondorGetLastSofrSequenceDealCall.builder()
                .reqType(actionType.getCondorValue())
                .seqType(seqType == null ? null : seqType.getName())
                .dealCode(processDealsInfo.getDealId())
                .requestId(processDealsInfo.getReqId())
                .build();
    }

}
