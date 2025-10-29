package ru.rshbintech.rsbankws.proxy.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ProxyRequestInfo {

    public static final ProxyRequestInfo WITHOUT_MAKE_MONITORING = new ProxyRequestInfo();

    private boolean needMakeMonitoring = false;
    private boolean needMakeFastAnswer = false;
    private XmlInfo soapEnvelopeXmlInfo;
    private XmlInfo methodCallXmlInfo;
    private XmlInfo processDealsXmlInfo;
    private ProcessDealsInfo processDealsInfo;
    private String seqId;
    private String fastAnswerXmlAsString;

}
