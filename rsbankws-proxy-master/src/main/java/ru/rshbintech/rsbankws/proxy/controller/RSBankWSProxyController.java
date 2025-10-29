package ru.rshbintech.rsbankws.proxy.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSClientException;
import ru.rshbintech.rsbankws.proxy.model.exception.RSBankWSProxyException;
import ru.rshbintech.rsbankws.proxy.service.RSBankWSProxyService;
import ru.rshbintech.rsbankws.proxy.service.error.ExchangeLoggingService;

import java.util.Map;

import static org.springframework.http.HttpStatus.NOT_FOUND;
import static ru.rshbintech.rsbankws.proxy.model.enums.ServiceType.RS_BANK_WS_PROXY;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.MSG_ERROR_PROXY;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.SOAP_CONTENT_TYPE;

@RestController
@RequiredArgsConstructor
@SuppressWarnings("unused")
@Tag(name = "API прокси-сервиса для RSBankWS")
public class RSBankWSProxyController {

    private final RSBankWSProxyService rsBankWSProxyService;
    private final ExchangeLoggingService exchangeLoggingService;

    @PostMapping(value = "${app.rs-bank-ws-proxy.uri}", produces = SOAP_CONTENT_TYPE, consumes = SOAP_CONTENT_TYPE)
    @Operation(
            summary = """
                    Метод производит перехват запроса для сервиса RSBankWS, генерацию быстрого ответа, если это
                    необходимо, а также проксирование запроса на сервис RSBankWS
                    """,
            responses = @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = String.class))
            )
    )
    public Mono<ResponseEntity<String>> rsBankWSProxyCall(@RequestBody String soapEnvelopeRequest,
                                                          @RequestHeader(required = false) HttpHeaders headers) {
        try {
            exchangeLoggingService.debugRequest(RS_BANK_WS_PROXY, headers, soapEnvelopeRequest);
            return rsBankWSProxyService.processRequest(soapEnvelopeRequest, headers);
        } catch (RSBankWSClientException e) {
            throw e;
        } catch (Exception e) {
            throw new RSBankWSProxyException(soapEnvelopeRequest, headers, MSG_ERROR_PROXY, e);
        }
    }

    @GetMapping(value = "${app.rs-bank-ws-proxy.uri}", produces = SOAP_CONTENT_TYPE)
    @Operation(
            summary = "Метод производит получение wsdl от сервиса RSBankWS",
            responses = @ApiResponse(
                    responseCode = "200",
                    content = @Content(schema = @Schema(implementation = String.class))
            ),
            parameters = @Parameter(
                    name = "wsdl",
                    description = "Параметр, который указывает, что надо получить WSDL (регистронезависим)",
                    required = true
            )
    )
    public Mono<ResponseEntity<String>> rsBankWSWsdlCall(
            @Parameter(hidden = true) @RequestParam Map<String, String> params,
            @RequestHeader(required = false) HttpHeaders headers) {
        if (!checkWsdlParam(params)) {
            throw new ResponseStatusException(NOT_FOUND);
        }
        try {
            exchangeLoggingService.debugRequest(RS_BANK_WS_PROXY, headers);
            return rsBankWSProxyService.processWsdlRequest(headers);
        } catch (RSBankWSClientException e) {
            throw e;
        } catch (Exception e) {
            throw new RSBankWSProxyException(headers, MSG_ERROR_PROXY, e);
        }
    }

    private boolean checkWsdlParam(@Nullable Map<String, String> params) {
        return MapUtils.isNotEmpty(params)
                && params.size() == 1
                && params.keySet().stream().anyMatch(param -> StringUtils.equalsIgnoreCase(param, "wsdl"));
    }

}
