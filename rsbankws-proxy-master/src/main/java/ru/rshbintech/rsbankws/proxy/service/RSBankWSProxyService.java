package ru.rshbintech.rsbankws.proxy.service;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import ru.rshbintech.rsbankws.proxy.client.RSBankWSClient;
import ru.rshbintech.rsbankws.proxy.configuration.properties.ProxyProperties;
import ru.rshbintech.rsbankws.proxy.model.ProxyRequestInfo;
import ru.rshbintech.rsbankws.proxy.service.error.ExchangeLoggingService;
import ru.rshbintech.rsbankws.proxy.service.xml.RSBankWSRequestForFastAnswerXmlService;

import static org.springframework.http.HttpStatus.OK;
import static ru.rshbintech.rsbankws.proxy.model.enums.ServiceType.RS_BANK_WS_PROXY;

@Service
@RequiredArgsConstructor
public class RSBankWSProxyService {

    private final RSBankWSClient rsBankWSClient;
    private final ProxyProperties proxyProperties;
    private final MonitoringService monitoringService;
    private final FastAnswerService fastAnswerService;
    private final HttpHeadersService httpHeadersService;
    private final ExchangeLoggingService exchangeLoggingService;
    private final RSBankWSRequestForFastAnswerXmlService rsBankWSRequestForFastAnswerXmlService;

    @NonNull
    public Mono<ResponseEntity<String>> processRequest(@NonNull String soapEnvelopeRequest,
                                                       @Nullable HttpHeaders headers) {
        if (proxyProperties.isProxyAllRequests()) {
            return rsBankWSClient.callRSBankWSWithResponse(soapEnvelopeRequest, headers);
        }
        final ProxyRequestInfo proxyRequestInfo = fastAnswerService.evaluateNeedMakeFastAnswer(
                monitoringService.evaluateNeedMakeMonitoring(soapEnvelopeRequest)
        );
        if (!proxyRequestInfo.isNeedMakeFastAnswer()) {
            if (proxyRequestInfo.isNeedMakeMonitoring()) {
                monitoringService.makeMonitoring(proxyRequestInfo);
            }
            return rsBankWSClient.callRSBankWSWithResponse(soapEnvelopeRequest, headers);
        }
        fastAnswerService.makeFastAnswer(proxyRequestInfo);
        rsBankWSClient.callRSBankWSWithoutResponse(
                rsBankWSRequestForFastAnswerXmlService.makeRSBankWSRequestXmlAsString(proxyRequestInfo),
                headers
        );
        final HttpStatusCode fastAnswerHttpStatus = OK;
        final HttpHeaders fastAnswerHeaders = httpHeadersService.prepareHttpHeadersForAnswer();
        final String fastAnswerXmlAsString = proxyRequestInfo.getFastAnswerXmlAsString();
        exchangeLoggingService.debugResponse(
                RS_BANK_WS_PROXY,
                fastAnswerHttpStatus,
                fastAnswerHeaders,
                fastAnswerXmlAsString
        );
        return Mono.just(
                ResponseEntity
                        .status(fastAnswerHttpStatus)
                        .headers(fastAnswerHeaders)
                        .body(fastAnswerXmlAsString)
        );
    }

    @NonNull
    public Mono<ResponseEntity<String>> processWsdlRequest(@Nullable HttpHeaders headers) {
        return rsBankWSClient.callRSBankWSWsdl(headers);
    }

}
