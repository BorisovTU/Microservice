package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Конфигурационные свойства Kafka, загружаемые из application.yaml.
 * Содержит настройки для потребителей (consumers) и производителей (producers).
 */
@Data
@Component
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaProperties {

    /**
     * Список конфигураций потребителей Kafka.
     * Каждый потребитель настраивается для конкретного топика с уникальными параметрами.
     */
    private List<ConsumerConfig> consumers;

    /**
     * Список конфигураций производителей Kafka.
     * Каждый производитель настраивается для конкретного топика с уникальными параметрами.
     */
    private List<ProducerConfig> producers;

    /**
     * Конфигурация потребителя Kafka для конкретного топика.
     */
    @Data
    public static class ConsumerConfig {
        /**
         * Название топика Kafka
         */
        private String topic;
        /**
         * Bootstrap servers для подключения к кластеру Kafka
         */
        private String bootstrapServers;
        /**
         * Group ID потребителя
         */
        private String groupId;
    }

    /**
     * Конфигурация производителя Kafka для конкретного топика.
     */
    @Data
    public static class ProducerConfig {
        /**
         * Название топика Kafka
         */
        private String topic;
        /**
         * Bootstrap servers для подключения к кластеру Kafka
         */
        private String bootstrapServers;
    }
}
