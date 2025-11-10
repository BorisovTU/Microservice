package ru.rshbintech.rsbankws.proxy.service.fastanswer;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerProperties;
import ru.rshbintech.rsbankws.proxy.model.dto.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.service.xml.FastAnswerXmlService;
import ru.rshbintech.rsbankws.proxy.service.database.CondorSofrBufferTableService;

@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(FastAnswerProperties.class)
public class FastAnswerService {

    private final FastAnswerProperties fastAnswerProperties;
    private final FastAnswerXmlService fastAnswerXmlService;
    private final CondorSofrBufferTableService condorSofrBufferTableService;
    private final FastAnswerEvaluationService evaluationService;

    @NonNull
    public ProxyRequestInfo evaluateNeedMakeFastAnswer(@NonNull ProxyRequestInfo proxyRequestInfo) {
        return evaluationService.evaluate(proxyRequestInfo, fastAnswerProperties);
    }

    @NonNull
    public void makeFastAnswer(@NonNull ProxyRequestInfo proxyRequestInfo) {
        proxyRequestInfo.setSeqId(
                condorSofrBufferTableService.callCondorGetLastSofrSequenceDealForFastAnswer(proxyRequestInfo)
        );
        proxyRequestInfo.setFastAnswerXmlAsString(
                fastAnswerXmlService.makeFastAnswerXmlAsString(proxyRequestInfo)
        );
    }
}
