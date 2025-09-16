package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReqDTO;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {
    private final CustomKafkaProperties kafkaProperties;

    public KafkaConfig(CustomKafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getConsumer().getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaProperties.getConsumer().getGroupId());
        props.putAll(kafkaProperties.getConsumer().getProperties());
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(kafkaProperties.getConsumer().getConcurrency());
        return factory;
    }

    @Bean
    public ProducerFactory<String, SendCorpActionsAssignmentReqDTO> producerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.getProducer().getProperties());

        // Set defaults if not specified in properties
        props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getProducer().getBootstrapServers());
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, SendCorpActionsAssignmentReqDTO> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
