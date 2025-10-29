package ru.rshbintech.rsbankws.proxy.service.xml;

import org.apache.commons.text.StringEscapeUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_RESPONSE_PATTERN_FAULT_METHOD_RESPONSE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlPatternConstants.XML_RESPONSE_PATTERN_XML_RPC_CALL_RESPONSE;

@Service
public class FaultAnswerXmlService {

    @NonNull
    public String makeFaultAnswerXmlAsString(int faultType, int faultCode, @NonNull String faultString) {
        return makeXMLRPCCallResponseXmlAsString(
                makeFaultMethodResponseXmlAsString(faultType, faultCode, faultString)
        );
    }

    @NonNull
    private String makeXMLRPCCallResponseXmlAsString(@NonNull String faultMethodResponseXmlAsString) {
        return String.format(
                XML_RESPONSE_PATTERN_XML_RPC_CALL_RESPONSE,
                faultMethodResponseXmlAsString
        );
    }

    private String makeFaultMethodResponseXmlAsString(int faultType, int faultCode, @NonNull String faultString) {
        return String.format(
                XML_RESPONSE_PATTERN_FAULT_METHOD_RESPONSE,
                faultType,
                faultCode,
                StringEscapeUtils.escapeXml10(faultString)
        );
    }

}
