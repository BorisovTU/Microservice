package ru.rshbintech.rsbankws.proxy.service;

import org.springframework.http.HttpHeaders;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import static org.springframework.http.HttpHeaders.CONTENT_TYPE;
import static ru.rshbintech.rsbankws.proxy.utils.RSBankWSProxyConstants.SOAP_CONTENT_TYPE;

@Service
public class HttpHeadersService {

    @NonNull
    public HttpHeaders prepareHttpHeadersForAnswer() {
        HttpHeaders headers = new org.springframework.http.HttpHeaders();
        /*
        На вызывающий SOAP клиент (АСУДР) нужно отдать 2 заголовка:
        1. Content-Type.
        2. Content-Length.
        Заголовок Content-Length будет установлен автоматически Spring'ом при обработке ответа в Servlet фильтрах
        после выхода из контроллера, поэтому нужно вручную установить только заголовок Content-Type.
         */
        headers.add(CONTENT_TYPE, SOAP_CONTENT_TYPE);
        return headers;
    }

}
