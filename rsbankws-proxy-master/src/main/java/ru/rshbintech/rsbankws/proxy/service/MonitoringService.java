package ru.rshbintech.rsbankws.proxy.service;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.configuration.properties.fastanswer.FastAnswerProperties;
import ru.rshbintech.rsbankws.proxy.model.ProcessDealsInfo;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.model.XmlInfo;
import ru.rshbintech.rsbankws.proxy.service.xml.XmlService;

import static ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo.WITHOUT_MAKE_MONITORING;
import static ru.rshbintech.rsbankws.proxy.model.enums.ActionType.getActionType;
import static ru.rshbintech.rsbankws.proxy.model.enums.XmlType.METHOD_CALL;
import static ru.rshbintech.rsbankws.proxy.model.enums.XmlType.PROCESS_DEALS;
import static ru.rshbintech.rsbankws.proxy.model.enums.XmlType.SOAP_ENVELOPE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_METHOD_CALL_METHOD_NAME;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_METHOD_CALL_TO_PROCESS_DEALS_XML;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_ACTION_TYPE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_DEAL_ID;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_PROCESS_DEALS_REQ_ID;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_SOAP_ENVELOPE_TO_METHOD_CALL_XML;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSXmlConstants.X_PATH_SOAP_ENVELOPE_XML_RPC_CALL;

@Service
@RequiredArgsConstructor
@EnableConfigurationProperties(FastAnswerProperties.class)
public class MonitoringService {

    private final XmlService xmlService;
    private final FastAnswerProperties fastAnswerProperties;
    private final CondorSofrBufferTableService condorSofrBufferTableService;

    @NonNull
    public ProxyRequestInfo evaluateNeedMakeMonitoring(@NonNull String soapEnvelopeXmlAsString) {
        if (CollectionUtils.isEmpty(fastAnswerProperties.getMethods())) {
            return WITHOUT_MAKE_MONITORING;
        }
        final XmlInfo soapEnvelopeXmlInfo = xmlService.parseXml(soapEnvelopeXmlAsString, SOAP_ENVELOPE);
        if (xmlService.isTagNotExists(soapEnvelopeXmlInfo, X_PATH_SOAP_ENVELOPE_XML_RPC_CALL)) {
            return WITHOUT_MAKE_MONITORING;
        }
        final XmlInfo methodCallXmlInfo = xmlService.getNestedXmlInfo(
                soapEnvelopeXmlInfo,
                METHOD_CALL,
                X_PATH_SOAP_ENVELOPE_TO_METHOD_CALL_XML
        );
        xmlService.checkTagExistsWithThrowError(methodCallXmlInfo, X_PATH_METHOD_CALL_METHOD_NAME);
        final String methodName = xmlService.getTagValueAsString(methodCallXmlInfo, X_PATH_METHOD_CALL_METHOD_NAME);
        if (!fastAnswerProperties.getMethods().contains(methodName)) {
            return WITHOUT_MAKE_MONITORING;
        }
        final XmlInfo processDealsXmlInfo = xmlService.getNestedXmlInfo(
                methodCallXmlInfo,
                PROCESS_DEALS,
                X_PATH_METHOD_CALL_TO_PROCESS_DEALS_XML
        );
        ProxyRequestInfo proxyRequestInfo = new ProxyRequestInfo();
        proxyRequestInfo.setNeedMakeMonitoring(true);
        proxyRequestInfo.setSoapEnvelopeXmlInfo(soapEnvelopeXmlInfo);
        proxyRequestInfo.setMethodCallXmlInfo(methodCallXmlInfo);
        proxyRequestInfo.setProcessDealsXmlInfo(processDealsXmlInfo);
        proxyRequestInfo.setProcessDealsInfo(prepareProcessDealsInfo(processDealsXmlInfo));
        return proxyRequestInfo;
    }

    @NonNull
    public void makeMonitoring(@NonNull ProxyRequestInfo proxyRequestInfo) {
        condorSofrBufferTableService.callCondorGetLastSofrSequenceDealForMonitoring(proxyRequestInfo);
    }

    @NonNull
    private ProcessDealsInfo prepareProcessDealsInfo(@NonNull XmlInfo processDealsXmlInfo) {
        ProcessDealsInfo processDealsInfo = new ProcessDealsInfo();
        processDealsInfo.setReqId(
                xmlService.getTagValueAsStringWithExistsAndEmptyCheck(
                        processDealsXmlInfo,
                        X_PATH_PROCESS_DEALS_REQ_ID
                )
        );
        processDealsInfo.setActionType(
                getActionType(
                        xmlService.getTagValueAsStringWithExistsAndEmptyCheck(
                                processDealsXmlInfo, X_PATH_PROCESS_DEALS_ACTION_TYPE
                        )
                )
        );
        processDealsInfo.setDealId(
                xmlService.getTagValueAsStringWithExistsAndEmptyCheck(
                        processDealsXmlInfo,
                        X_PATH_PROCESS_DEALS_DEAL_ID
                )
        );
        return processDealsInfo;
    }

}
