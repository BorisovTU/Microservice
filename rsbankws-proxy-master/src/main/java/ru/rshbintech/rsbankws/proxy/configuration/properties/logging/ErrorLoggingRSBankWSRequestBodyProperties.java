package ru.rshbintech.rsbankws.proxy.configuration.properties.logging;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.error.logging.rs-bank-ws.request.body")
public class ErrorLoggingRSBankWSRequestBodyProperties extends AbstractErrorLoggingProperties {
}
