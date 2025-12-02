package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "app.routing")
public class DatabaseRoutingProperties {

    private Map<String, String> incoming;
    private Map<String, Integer> outgoing;
}
