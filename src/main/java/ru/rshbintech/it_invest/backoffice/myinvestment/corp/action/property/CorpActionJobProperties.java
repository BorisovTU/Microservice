package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "corp-action.job")
@Getter
@Setter
public class CorpActionJobProperties {
    private long consumerRate;
    private int consumerBatchSize;
    private long producerRate;
    private int producerBatchSize;
}
