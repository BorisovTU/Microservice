package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Setter
@Getter
@Component
@ConfigurationProperties(prefix = "kafka.corp-action")
public class CustomKafkaProperties {
    private String fromDiasoftTopic;
    private String fromDiasoftDlqTopic;
    private String notificationFromDiasoftTopic;
    private String postInstructionFromSiTopic;
    private String instructionToDiasoftTopic;

    private Consumer consumer = new Consumer();
    private Producer producer = new Producer();

    @Setter
    @Getter
    public static class Consumer {
        private String bootstrapServers;
        private String groupId;
        private int concurrency;
        private Map<String, Object> properties = new HashMap<>();
    }

    @Getter
    @Setter
    public static class Producer {
        private String bootstrapServers;
        private Map<String, Object> properties = new HashMap<>();
    }
}