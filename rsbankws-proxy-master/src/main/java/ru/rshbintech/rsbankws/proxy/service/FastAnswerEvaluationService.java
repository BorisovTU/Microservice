package ru.rshbintech.rsbankws.proxy.service.fastanswer;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerDealRuleProperties;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerProperties;
import ru.rshbintech.rsbankws.proxy.model.dto.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.service.xml.XmlService;

import java.util.Objects;

import static ru.rshbintech.rsbankws.proxy.model.enums.ActionType.NEW;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.*;

@Service
@RequiredArgsConstructor
public class FastAnswerEvaluationService {

    private final XmlService xmlService;

    @NonNull
    public ProxyRequestInfo evaluate(@NonNull ProxyRequestInfo proxyRequestInfo,
                                     @NonNull FastAnswerProperties fastAnswerProperties) {
        var processDealsInfo = proxyRequestInfo.getProcessDealsInfo();

        if (!Objects.equals(NEW, processDealsInfo.getActionType()) ||
                CollectionUtils.isEmpty(fastAnswerProperties.getDealRules())) {
            return proxyRequestInfo;
        }

        var processDealsXmlInfo = proxyRequestInfo.getProcessDealsXmlInfo();
        processDealsInfo.setDealType(
                xmlService.getOnlyOneChildTagNameForCurrentTag(processDealsXmlInfo, X_PATH_PROCESS_DEALS_DEAL_DATA)
        );

        FastAnswerDealRuleProperties dealRule = fastAnswerProperties.getDealRules()
                .get(processDealsInfo.getDealType());

        if (dealRule == null || !matchesDealKind(processDealsInfo, dealRule, processDealsXmlInfo)) {
            return proxyRequestInfo;
        }

        prepareForFastAnswer(proxyRequestInfo, processDealsXmlInfo, dealRule);
        return proxyRequestInfo;
    }

    private boolean matchesDealKind(ProcessDealsInfo processDealsInfo,
                                    FastAnswerDealRuleProperties dealRule,
                                    var processDealsXmlInfo) {
        processDealsInfo.setDealKind(
                xmlService.getTagValueAsStringWithExistsAndEmptyCheck(
                        processDealsXmlInfo, X_PATH_PROCESS_DEALS_DEAL_KIND
                )
        );

        String dealKindFromSettings = dealRule.getKind();
        if (StringUtils.isEmpty(dealKindFromSettings)) {
            return true;
        }

        if (StringUtils.startsWith(dealKindFromSettings, "!")) {
            return !StringUtils.equals(
                    processDealsInfo.getDealKind(),
                    StringUtils.substring(dealKindFromSettings, 1)
            );
        }

        return StringUtils.equals(processDealsInfo.getDealKind(), dealKindFromSettings);
    }

    private void prepareForFastAnswer(ProxyRequestInfo proxyRequestInfo,
                                      var processDealsXmlInfo,
                                      FastAnswerDealRuleProperties dealRule) {
        var processDealsInfo = proxyRequestInfo.getProcessDealsInfo();

        proxyRequestInfo.setNeedMakeFastAnswer(true);
        processDealsInfo.setSenderId(
                xmlService.getTagValueAsString(processDealsXmlInfo, X_PATH_PROCESS_DEALS_SENDER_ID)
        );
        processDealsInfo.setExternalId(
                xmlService.getTagValueAsString(processDealsXmlInfo, X_PATH_PROCESS_DEALS_EXTERNAL_ID)
        );
        processDealsInfo.setSeqType(dealRule.getSeqType());
    }
}
