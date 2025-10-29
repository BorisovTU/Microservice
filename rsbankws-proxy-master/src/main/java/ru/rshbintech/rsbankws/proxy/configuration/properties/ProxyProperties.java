package ru.rshbintech.rsbankws.proxy.configuration.properties;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.rs-bank-ws-proxy")
public class ProxyProperties {

    /**
     * URI для обращения к прокси сервису RSBankWS
     */
    @NotEmpty
    private String uri = "/ws/RSBankWS.asmx";

    /**
     * Нужно ли проксировать все запросы, которые поступают на сервис независимо от настроек быстрого ответа.
     */
    private boolean proxyAllRequests = false;

}
