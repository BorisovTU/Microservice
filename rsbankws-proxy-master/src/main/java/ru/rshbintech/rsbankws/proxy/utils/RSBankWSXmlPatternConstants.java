package ru.rshbintech.rsbankws.proxy.utils;

import lombok.experimental.UtilityClass;

@UtilityClass
public class RSBankWSXmlPatternConstants {

    public static final String XML_RESPONSE_PATTERN_XML_RPC_CALL_RESPONSE =
            "<?xml version='1.0' encoding='UTF-8'?><S:Envelope xmlns:S=\"http://schemas.xmlsoap.org/soap/envelope/\"><S:Body><ns2:XMLRPCCallResponse xmlns:ns2=\"http://rsbank.softlab.ru/\"><return>%s</return></ns2:XMLRPCCallResponse></S:Body></S:Envelope>";

    public static final String XML_RESPONSE_PATTERN_METHOD_RESPONSE = """
            <?xml version='1.0' encoding='UTF-8'?><methodResponse xmlns="http://www.softlab.ru/xml-rpc/schema" xmlns:n0="http://www.softlab.ru/xml-rpc/schema" n0:reqId="%s" n0:logicalId=""><params><param><value>
            <string>%s</string>
            </value></param></params></methodResponse>""";

    public static final String XML_RESPONSE_PATTERN_PROCESS_DEALS = """
            <?xml version='1.0' encoding='UTF-8'?><ProcessDeals_resp>
            <ReqID>%s</ReqID>
            <SenderId>%s</SenderId>
            <Result>
            <Code>0</Code>
            <Text>OK</Text>
            <DealList>
            <DealParm>
            <ExternalID>%s</ExternalID>
            <DealID>%s</DealID>
            <SOFRDealID>%s</SOFRDealID>
            <Result>
            <Code>0</Code>
            <Text>OK</Text>
            </Result>
            </DealParm>
            </DealList>
            </Result>
            </ProcessDeals_resp>
            """;

    public static final String XML_REQUEST_PATTERN_XML_RPC_CALL = """
            <?xml version='1.0' encoding='UTF-8'?>
            <soapenv:Envelope xmlns:rsb="http://rsbank.softlab.ru/"
                              xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
               <soapenv:Header/>
               <soapenv:Body>
                  <rsb:XMLRPCCall>
                     <arg0><![CDATA[%s]]></arg0>
                     <arg1>%s</arg1>
                     <arg2>%s</arg2>
                  </rsb:XMLRPCCall>
               </soapenv:Body>
            </soapenv:Envelope>
            """;

    public static final String XML_REQUEST_PATTERN_METHOD_CALL = """
            <?xml version='1.0' encoding='UTF-8'?>
            <methodCall xmlns="http://www.softlab.ru/xml-rpc/schema"
                        xmlns:r="http://www.softlab.ru/xml-rpc/schema"
                        r:reqId="%s">
            <methodName>RunMacro.ws_ProcessDeals.ProcessDeals</methodName>
                <params>
                    <param>
                        <value>
                            <string>%s</string>
                        </value>
                    </param>
                </params>
            </methodCall>
            """;

    public static final String XML_RESPONSE_PATTERN_FAULT_METHOD_RESPONSE = """
            <?xml version='1.0' encoding='UTF-8'?>
            <methodResponse xmlns="http://www.softlab.ru/xml-rpc/schema"
                            xmlns:n0="http://www.softlab.ru/xml-rpc/schema"
                            n0:reqId="">
                <fault>
                    <value>
                        <struct>
                            <member>
                                <name>faultType</name>
                                <value>
                                    <integer>%d</integer>
                                </value>
                            </member>
                            <member>
                                <name>faultCode</name>
                                <value>
                                    <integer>%d</integer>
                                </value>
                            </member>
                            <member>
                                <name>faultString</name>
                                <value>
                                    <string>%s</string>
                                </value>
                            </member>
                        </struct>
                    </value>
                </fault>
            </methodResponse>
            """;


}
