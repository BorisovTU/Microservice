package ru.rshbintech.rsbankws.proxy.service.xml;

import lombok.RequiredArgsConstructor;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.model.ProcessDealsInfo;
import ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants;

import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_RESPONSE_PATTERN_METHOD_RESPONSE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_RESPONSE_PATTERN_PROCESS_DEALS;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_RESPONSE_PATTERN_XML_RPC_CALL_RESPONSE;

@Service
@RequiredArgsConstructor
public class FastAnswerXmlService {

    private final XmlService xmlService;

    @NonNull
    public String makeFastAnswerXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo) {
        return makeXMLRPCCallResponseXmlAsString(
                StringEscapeUtils.escapeXml10(
                        makeMethodResponseXmlAsString(
                                proxyRequestInfo,
                                makeProcessDealsRespXmlAsString(proxyRequestInfo)
                        )
                )
        );
    }

    @NonNull
    private String makeXMLRPCCallResponseXmlAsString(@NonNull String methodResponseXmlAsString) {
        return String.format(
                XML_RESPONSE_PATTERN_XML_RPC_CALL_RESPONSE,
                methodResponseXmlAsString
        );
    }

    @NonNull
    private String makeMethodResponseXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo,
                                                 @NonNull String processDealsRespXmlAsString) {
        final String reqId = xmlService.getAttributeValueAsString(
                proxyRequestInfo.getMethodCallXmlInfo(),
                RSBankWSXmlConstants.X_PATH_METHOD_CALL_REQ_ID_ATTRIBUTE
        );
        return String.format(
                XML_RESPONSE_PATTERN_METHOD_RESPONSE,
                xmlService.wrapReqIdAttrIfNeed(reqId),
                StringEscapeUtils.escapeXml10(processDealsRespXmlAsString)
        );
    }

    @NonNull
    private String makeProcessDealsRespXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo) {
        final ProcessDealsInfo processDealsInfo = proxyRequestInfo.getProcessDealsInfo();
        return String.format(
                XML_RESPONSE_PATTERN_PROCESS_DEALS,
                processDealsInfo.getReqId(),
                processDealsInfo.getSenderId(),
                processDealsInfo.getExternalId(),
                processDealsInfo.getDealId(),
                proxyRequestInfo.getSeqId()
        );
    }

}
