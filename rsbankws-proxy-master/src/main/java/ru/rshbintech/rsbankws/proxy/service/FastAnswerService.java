package ru.rshbintech.rsbankws.proxy.service;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerDealRuleProperties;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerProperties;
import ru.rshbintech.rsbankws.proxy.model.ProcessDealsInfo;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.model.XmlInfo;
import ru.rshbintech.rsbankws.proxy.service.xml.FastAnswerXmlService;
import ru.rshbintech.rsbankws.proxy.service.xml.XmlService;

import java.util.Objects;

import static ru.rshbintech.rsbankws.proxy.model.enums.ActionType.NEW;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_DEAL_DATA;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_DEAL_KIND;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_EXTERNAL_ID;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_SENDER_ID;

@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(FastAnswerProperties.class)
public class FastAnswerService {

    private final XmlService xmlService;
    private final FastAnswerProperties fastAnswerProperties;
    private final FastAnswerXmlService fastAnswerXmlService;
    private final CondorSofrBufferTableService condorSofrBufferTableService;

    @NonNull
    public ProxyRequestInfo evaluateNeedMakeFastAnswer(@NonNull ProxyRequestInfo proxyRequestInfo) {
        ProcessDealsInfo processDealsInfo = proxyRequestInfo.getProcessDealsInfo();
        if (!Objects.equals(NEW, processDealsInfo.getActionType())) {
            return proxyRequestInfo;
        }
        if (CollectionUtils.isEmpty(fastAnswerProperties.getDealRules())) {
            return proxyRequestInfo;
        }
        final XmlInfo processDealsXmlInfo = proxyRequestInfo.getProcessDealsXmlInfo();
        processDealsInfo.setDealType(
                xmlService.getOnlyOneChildTagNameForCurrentTag(
                        processDealsXmlInfo,
                        X_PATH_PROCESS_DEALS_DEAL_DATA
                )
        );
        final FastAnswerDealRuleProperties fastAnswerDealRule = fastAnswerProperties.getDealRules().get(
                processDealsInfo.getDealType()
        );
        if (fastAnswerDealRule == null) {
            return proxyRequestInfo;
        }
        processDealsInfo.setDealKind(
                xmlService.getTagValueAsStringWithExistsAndEmptyCheck(
                        processDealsXmlInfo,
                        X_PATH_PROCESS_DEALS_DEAL_KIND
                )
        );
        final String dealKindFromSettings = fastAnswerDealRule.getKind();
        if (StringUtils.isNotEmpty(dealKindFromSettings)) {
            if (StringUtils.startsWith(dealKindFromSettings, "!")) {
                if (StringUtils.equals(
                        processDealsInfo.getDealKind(),
                        StringUtils.substring(dealKindFromSettings, 1)
                )) {
                    return proxyRequestInfo;
                }
            } else if (!StringUtils.equals(processDealsInfo.getDealKind(), dealKindFromSettings)) {
                return proxyRequestInfo;
            }
        }
        proxyRequestInfo.setNeedMakeFastAnswer(true);
        processDealsInfo.setSenderId(
                xmlService.getTagValueAsString(processDealsXmlInfo, X_PATH_PROCESS_DEALS_SENDER_ID)
        );
        processDealsInfo.setExternalId(
                xmlService.getTagValueAsString(processDealsXmlInfo, X_PATH_PROCESS_DEALS_EXTERNAL_ID)
        );
        processDealsInfo.setSeqType(fastAnswerDealRule.getSeqType());
        return proxyRequestInfo;
    }

    @NonNull
    public void makeFastAnswer(@NonNull ProxyRequestInfo proxyRequestInfo) {
        proxyRequestInfo.setSeqId(
                condorSofrBufferTableService.callCondorGetLastSofrSequenceDealForFastAnswer(proxyRequestInfo)
        );
        proxyRequestInfo.setFastAnswerXmlAsString(fastAnswerXmlService.makeFastAnswerXmlAsString(proxyRequestInfo));
    }

}
