package ru.rshbintech.rsbankws.proxy.configuration.properties.logging;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.error.logging.rs-bank-ws.request.headers")
public class ErrorLoggingRSBankWSRequestHeadersProperties extends AbstractErrorLoggingProperties {
}
