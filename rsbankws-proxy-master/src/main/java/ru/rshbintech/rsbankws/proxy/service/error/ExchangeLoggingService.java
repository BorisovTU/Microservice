package ru.rshbintech.rsbankws.proxy.service.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.rshbintech.rsbankws.proxy.model.enums.ServiceType;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class ExchangeLoggingService {

    public void debugRequest(@NonNull ServiceType serviceType,
                             @Nullable HttpHeaders requestHeaders,
                             @Nullable String requestBody) {
        final List<String> requestAttributes = new ArrayList<>(2);
        if (requestHeaders != null && !requestHeaders.isEmpty()) {
            requestAttributes.add(String.format("headers = [%s]", requestHeaders));
        }
        if (StringUtils.isNotEmpty(requestBody)) {
            requestAttributes.add(String.format("body = [%s]", requestBody));
        }
        if (CollectionUtils.isEmpty(requestAttributes)) {
            log.debug("{} service request", serviceType.getName());
        } else {
            log.debug("{} service request: {}", serviceType.getName(), String.join(", ", requestAttributes));
        }
    }

    public void debugRequest(@NonNull ServiceType serviceType,
                             @Nullable HttpHeaders requestHeaders) {
        debugRequest(serviceType, requestHeaders, null);
    }

    public void debugResponse(@NonNull ServiceType serviceType,
                              @NonNull HttpStatusCode responseStatus,
                              @Nullable HttpHeaders responseHeaders,
                              @NonNull String responseBody) {
        log.debug(
                "{} service response: status = [{}], headers = [{}], body = [{}].",
                serviceType.getName(),
                responseStatus.value(),
                responseHeaders,
                responseBody
        );
    }

}
