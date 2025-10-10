package ru.rshbintech.itinvest.ckips.ips.sofr.qmngr.mq.lib.common.configuration.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.datasource")
public record DatasourceProperties(
        String url,
        String username,
        String password,
        String driverClassName
) {
}
