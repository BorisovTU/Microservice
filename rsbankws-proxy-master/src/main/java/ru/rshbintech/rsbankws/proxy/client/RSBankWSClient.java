package ru.rshbintech.rsbankws.proxy.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import ru.rshbintech.rsbankws.proxy.configuration.properties.RSBankWSClientProperties;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSClientException;
import ru.rshbintech.rsbankws.proxy.service.error.ExchangeLoggingService;
import ru.rshbintech.rsbankws.proxy.service.error.RSBankWSClientErrorLoggingService;

import static ru.rshbintech.rsbankws.proxy.model.enums.ServiceType.RS_BANK_WS;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.MSG_ERROR_PROXY;

@Slf4j
@Component
@RequiredArgsConstructor
public class RSBankWSClient {

    private static final String MSG_RS_BANK_WS_CALL_ERROR = "Ошибка вызова сервиса SOFR.RSBankWS";
    private static final String MSG_RS_BANK_WS_WSDL_GET_ERROR = "Ошибка получения wsdl от сервиса SOFR.RSBankWS";

    private final WebClient rsBankWSWebClient;
    private final ExchangeLoggingService exchangeLoggingService;
    private final RSBankWSClientProperties rsBankWSClientProperties;
    private final RSBankWSClientErrorLoggingService rsBankWSClientErrorLoggingService;

    /*
    При вызове без ожидания ответа ошибки не должны выбрасываться наружу (а должны просто логироваться), так как в
    данном сценарии быстрый ответ возвращается сразу на вызывающую сторону и ошибки вызова RSBankWS никак не должны
    влиять на эту логику.
     */
    @NonNull
    public void callRSBankWSWithoutResponse(@NonNull String soapEnvelopeRequest,
                                            @Nullable HttpHeaders headers) {
        try {
            createRSBankWSRequest(soapEnvelopeRequest, headers)
                    .retrieve()
                    .bodyToMono(Void.class)
                    .onErrorResume(
                            error -> {
                                logRSBankWSCallError(soapEnvelopeRequest, headers, error);
                                return Mono.empty();
                            }
                    )
                    .contextCapture()
                    .subscribe();
        } catch (Exception e) {
            logRSBankWSCallError(soapEnvelopeRequest, headers, e);
        }
    }

    /*
    При вызове с ожиданием ответа все ошибки вызова должны транслироваться на вызывающую сторону посредством
    Fault ответа, именно поэтому все ошибки пробрасываются на верх в виде исключения RSBankWSClientException
    и уже там перехватываются и обрабатываются ExceptionHandler'ом.
     */
    @NonNull
    public Mono<ResponseEntity<String>> callRSBankWSWithResponse(@NonNull String soapEnvelopeRequest,
                                                                 @Nullable HttpHeaders headers) {
        try {
            return createRSBankWSRequest(soapEnvelopeRequest, headers)
                    .exchangeToMono(this::clientResponseToResponseEntity)
                    .onErrorResume(throwable -> Mono.error(
                                    new RSBankWSClientException(
                                            soapEnvelopeRequest,
                                            headers,
                                            MSG_RS_BANK_WS_CALL_ERROR,
                                            throwable
                                    )
                            )
                    );
        } catch (Exception e) {
            throw new RSBankWSClientException(soapEnvelopeRequest, headers, MSG_RS_BANK_WS_CALL_ERROR, e);
        }
    }

    //Получение wsdl от сервиса RSBankWS
    @NonNull
    public Mono<ResponseEntity<String>> callRSBankWSWsdl(@Nullable HttpHeaders headers) {
        try {
            exchangeLoggingService.debugRequest(RS_BANK_WS, headers);
            return rsBankWSWebClient.get()
                    .uri(rsBankWSClientProperties.getUrl() + "?wsdl")
                    .headers(httpHeaders -> {
                        if (headers != null && !headers.isEmpty()) {
                            httpHeaders.addAll(headers);
                        }
                    })
                    .exchangeToMono(this::clientResponseToResponseEntity)
                    .onErrorResume(throwable -> Mono.error(
                                    new RSBankWSClientException(
                                            headers,
                                            MSG_RS_BANK_WS_WSDL_GET_ERROR,
                                            throwable
                                    )
                            )
                    );
        } catch (Exception e) {
            throw new RSBankWSClientException(headers, MSG_RS_BANK_WS_WSDL_GET_ERROR, e);
        }
    }

    @NonNull
    private WebClient.RequestHeadersSpec<?> createRSBankWSRequest(@NonNull String soapEnvelopeRequest,
                                                                  @Nullable HttpHeaders headers) {
        exchangeLoggingService.debugRequest(RS_BANK_WS, headers, soapEnvelopeRequest);
        return rsBankWSWebClient.post()
                .uri(rsBankWSClientProperties.getUrl())
                .body(BodyInserters.fromValue(soapEnvelopeRequest))
                .headers(httpHeaders -> {
                    if (headers != null && !headers.isEmpty()) {
                        httpHeaders.addAll(headers);
                    }
                });
    }

    private void logRSBankWSCallError(@NonNull String soapEnvelopeRequest,
                                      @Nullable HttpHeaders headers,
                                      @NonNull Throwable throwable) {
        log.error(MSG_ERROR_PROXY);
        log.error(
                rsBankWSClientErrorLoggingService.makeLogText(
                        soapEnvelopeRequest,
                        headers,
                        MSG_RS_BANK_WS_CALL_ERROR,
                        throwable
                )
        );
    }

    @NonNull
    private Mono<ResponseEntity<String>> clientResponseToResponseEntity(@NonNull ClientResponse response) {
        return response.bodyToMono(String.class).map(
                rsBankWSResponseBody -> {
                    final HttpStatusCode rsBankWSResponseHttpStatus = response.statusCode();
                    final HttpHeaders rsBankWSResponseHeaders = response.headers().asHttpHeaders();
                    exchangeLoggingService.debugResponse(
                            RS_BANK_WS,
                            rsBankWSResponseHttpStatus,
                            rsBankWSResponseHeaders,
                            rsBankWSResponseBody
                    );
                    return ResponseEntity
                            .status(rsBankWSResponseHttpStatus)
                            .headers(rsBankWSResponseHeaders)
                            .body(rsBankWSResponseBody);
                }
        );
    }

}
