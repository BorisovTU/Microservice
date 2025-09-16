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
@ConfigurationProperties(prefix = "kafka")
public class CustomKafkaProperties {
    private NotificationFromDiasoft notificationFromDiasoft = new NotificationFromDiasoft();
    private InternalNotification internalNotification = new InternalNotification();
    private InternalNotificationOwnerBalance internalNotificationOwnerBalance = new InternalNotificationOwnerBalance();
    private InternalInstruction internalInstruction = new InternalInstruction();
    private InstructionToDiasoft instructionToDiasoft = new InstructionToDiasoft();

    @Setter
    @Getter
    public static class NotificationFromDiasoft {
        private String topic;
        private String topicDlq;
        private Consumer consumer = new Consumer();
    }

    @Setter
    @Getter
    public static class InternalNotification {
        private String topic;
        private Consumer consumer = new Consumer();
        private Producer producer = new Producer();
    }

    @Setter
    @Getter
    public static class InternalInstruction {
        private String topic;
        private Consumer consumer = new Consumer();
        private Producer producer = new Producer();
    }

    @Setter
    @Getter
    public static class InternalNotificationOwnerBalance {
        private String topic;
        private Consumer consumer = new Consumer();
    }

    @Setter
    @Getter
    public static class InstructionToDiasoft {
        private String topic;
        private Producer producer = new Producer();
    }

    @Setter
    @Getter
    public static class Consumer {
        private String bootstrapServers;
        private String groupId;
        private int concurrency;
        private String keyDeserializer;
        private String valueDeserializer;
        private String trustedPackages;
        private Map<String, Object> properties = new HashMap<>();
    }

    @Getter
    @Setter
    public static class Producer {
        private String bootstrapServers;
        private Security security = new Security();
        private ProducerConfig producer = new ProducerConfig();

        @Setter
        @Getter
        public static class Security {
            private String protocol;
        }

        @Setter
        @Getter
        public static class ProducerConfig {
            private int retries;
            private String keySerializer;
            private String valueSerializer;
            private String acks;
            private Map<String, Object> properties = new HashMap<>();
        }
    }
}