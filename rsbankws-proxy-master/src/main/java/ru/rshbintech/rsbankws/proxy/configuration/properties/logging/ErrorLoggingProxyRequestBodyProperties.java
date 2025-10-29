package ru.rshbintech.rsbankws.proxy.configuration.properties.logging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.error.logging.proxy.request.body")
public class ErrorLoggingProxyRequestBodyProperties extends AbstractErrorLoggingProperties {
}
