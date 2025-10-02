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
    private String streamTopicPrefix;
    private String streamTopicClientBalancesGlobal;
    private ConsumerConfig notificationFromDiasoft = new ConsumerConfig();
    private ConsumerProducerConfig internalNotification = new ConsumerProducerConfig();
    private ConsumerConfig internalNotificationOwnerBalance = new ConsumerConfig();
    private ConsumerProducerConfigDlq internalInstruction = new ConsumerProducerConfigDlq();
    private ProducerConfig instructionToDiasoft = new ProducerConfig();
    private ConsumerConfig internalInstructionView = new ConsumerConfig();
    private StreamConsumerProducerConfig internalInstructionBalance = new StreamConsumerProducerConfig();

    public String ClientBalancesStore;

    // Базовый класс для конфигураций с Consumer
    @Setter
    @Getter
    public static class ConsumerConfig {
        private String topic;
        private String topicDlq; // Опциональное поле для DLQ
        private Consumer consumer = new Consumer();
    }

    // Базовый класс для конфигураций с Producer
    @Setter
    @Getter
    public static class ProducerConfig {
        private String topic;
        private Producer producer = new Producer();
    }

    @Getter
    @Setter
    public static class StreamConsumerProducerConfig {
        private String topic;
        private ShortConsumer consumer = new ShortConsumer();
        private Producer producer = new Producer();
    }

    // Базовый класс для конфигураций с Consumer и Producer
    @Setter
    @Getter
    public static class ConsumerProducerConfig {
        private String topic;
        private Consumer consumer = new Consumer();
        private Producer producer = new Producer();
    }

    @Setter
    @Getter
    public static class ConsumerProducerConfigDlq extends ConsumerProducerConfig {
        private String topicDlq;
    }

    @Setter
    @Getter
    public static class ShortConsumer {
        private String bootstrapServers;
        private String groupId;
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