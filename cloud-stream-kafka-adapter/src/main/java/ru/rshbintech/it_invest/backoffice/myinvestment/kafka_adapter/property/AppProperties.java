package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    private long waitMsgMillis = 5000;
}
