package ru.rshbintech.rsbankws.proxy.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProxyRequestInfo {
    private boolean needMakeMonitoring = false;
    private boolean needMakeFastAnswer = false;
    private String seqId;
    private String fastAnswerXmlAsString;

    private XmlInfo soapEnvelopeXmlInfo;
    private XmlInfo methodCallXmlInfo;
    private XmlInfo processDealsXmlInfo;
    private ProcessDealsInfo processDealsInfo;

    public static final ProxyRequestInfo WITHOUT_MAKE_MONITORING = new ProxyRequestInfo();
}
