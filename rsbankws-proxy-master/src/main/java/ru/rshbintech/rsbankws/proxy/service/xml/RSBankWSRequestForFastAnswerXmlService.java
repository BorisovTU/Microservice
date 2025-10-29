package ru.rshbintech.rsbankws.proxy.service.xml;

import lombok.RequiredArgsConstructor;
import org.apache.commons.text.StringEscapeUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.model.XmlInfo;

import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_METHOD_CALL_REQ_ID_ATTRIBUTE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_IDENTITY_DEAL;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_SOAP_ENVELOPE_PRIORITY;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_SOAP_ENVELOPE_TIMEOUT;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_REQUEST_PATTERN_METHOD_CALL;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_REQUEST_PATTERN_XML_RPC_CALL;

@Service
@RequiredArgsConstructor
public class RSBankWSRequestForFastAnswerXmlService {

    private final XmlService xmlService;

    @NonNull
    public String makeRSBankWSRequestXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo) {
        return makeXMLRPCCallXmlAsString(
                proxyRequestInfo,
                makeMethodCallXmlAsString(
                        proxyRequestInfo,
                        makeProcessDealsReqXmlAsString(proxyRequestInfo)
                )
        );
    }

    @NonNull
    private String makeXMLRPCCallXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo,
                                             @NonNull String methodCallXmlAsString) {
        final XmlInfo soapEnvelopeXmlInfo = proxyRequestInfo.getSoapEnvelopeXmlInfo();
        return String.format(
                XML_REQUEST_PATTERN_XML_RPC_CALL,
                methodCallXmlAsString,
                xmlService.getTagValueAsString(soapEnvelopeXmlInfo, X_PATH_SOAP_ENVELOPE_TIMEOUT),
                xmlService.getTagValueAsString(soapEnvelopeXmlInfo, X_PATH_SOAP_ENVELOPE_PRIORITY)
        );
    }

    @NonNull
    private String makeMethodCallXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo,
                                             @NonNull String processDealsReqXmlAsString) {
        final String reqId = xmlService.getAttributeValueAsString(
                proxyRequestInfo.getMethodCallXmlInfo(),
                X_PATH_METHOD_CALL_REQ_ID_ATTRIBUTE
        );
        return String.format(
                XML_REQUEST_PATTERN_METHOD_CALL,
                xmlService.wrapReqIdAttrIfNeed(reqId),
                StringEscapeUtils.escapeXml10(processDealsReqXmlAsString)
        );
    }

    @NonNull
    private String makeProcessDealsReqXmlAsString(@NonNull ProxyRequestInfo proxyRequestInfo) {
        final XmlInfo processDealsXmlInfo = proxyRequestInfo.getProcessDealsXmlInfo();
        xmlService.addNewTag(
                processDealsXmlInfo,
                X_PATH_PROCESS_DEALS_IDENTITY_DEAL,
                "SofrGenID",
                proxyRequestInfo.getSeqId()
        );
        return xmlService.writeXmlAsString(processDealsXmlInfo);
    }

}
