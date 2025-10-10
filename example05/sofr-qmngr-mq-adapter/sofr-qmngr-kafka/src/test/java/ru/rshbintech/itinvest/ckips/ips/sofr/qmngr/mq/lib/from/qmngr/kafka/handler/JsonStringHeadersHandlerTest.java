package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.from.qmngr.kafka.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JsonStringHeadersHandlerTest {

    @Test
    void testGenerateKafkaHeaders(){
        //given
        String headersString = """
                {
                    "x-system-from": "SOFR",
                    "x-system-to": "SINV",
                    "x-template-name": "individual-brokerage-report",
                    "x-bucket-name": "ips-document-factory",
                    "x-template-type": "jrxml",
                    "x-output-type": "PDF",
                    "x-service-name": "GetBrokerageReportInfo",
                    "x-trace-id": "237A977204740A63E063052C070A25CC",
                    "x-request-id": "23DDF230-1C1B-1EA7-E063-052C070A4E4C",
                    "x-request-time": "2024-10-07T09:15:23.469",
                    "x-output-file-name": "00/02-195048450109202431102024",
                    "x-test-boolean": true,
                    "x-test-number": 123.123,
                    "x-template-params": [
                        {
                            "paramName": "df_facsimile",
                            "paramType": "FILE",
                            "paramValue": "FedorovaEN.png"
                        }
                    ]
                }""";
        JsonStringHeadersHandler jsonStringHeadersHandler = new JsonStringHeadersHandler(new ObjectMapper());

        //when
        List<Header> kafkaHeaders = jsonStringHeadersHandler.toKafkaHeaders(headersString);
        var xTemplateParams = kafkaHeaders.stream().filter(h -> "x-template-params".equals(h.key())).findFirst().get();

        //then
        assertEquals(14, kafkaHeaders.size());
        assertEquals("[{\"paramName\":\"df_facsimile\",\"paramType\":\"FILE\",\"paramValue\":\"FedorovaEN.png\"}]", new String(xTemplateParams.value()));
    }

}