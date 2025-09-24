package ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionInstructionRequest;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.CorporateActionNotificationDto;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto.SendCorpActionsAssignmentReq;
import ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.property.CustomKafkaProperties;

import java.util.HashMap;
import java.util.Map;

import static ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.constant.BeanConstants.*;

@Configuration
@RequiredArgsConstructor
public class KafkaConfig {
    private final CustomKafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper;

    @Bean
    public ConsumerFactory<String, String> notificationFromDiasoftConsumerFactory() {
        return createConsumerFactory(kafkaProperties.getNotificationFromDiasoft().getConsumer());
    }

    @Bean
    public ConsumerFactory<String, CorporateActionInstructionRequest> instructionViewConsumerFactory() {
        JsonDeserializer<CorporateActionInstructionRequest> deserializer = new JsonDeserializer<>(CorporateActionInstructionRequest.class, objectMapper);
        deserializer.addTrustedPackages("ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto");
        return createConsumerFactory(kafkaProperties.getInternalInstructionView().getConsumer(), new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String, CorporateActionNotificationDto> internalNotificationConsumerFactory() {
        JsonDeserializer<CorporateActionNotificationDto> deserializer = new JsonDeserializer<>(CorporateActionNotificationDto.class, objectMapper);
        deserializer.addTrustedPackages("ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto");
        return createConsumerFactory(kafkaProperties.getInternalNotification().getConsumer(), new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String, CorporateActionNotificationDto> internalNotificationOwnerBalanceConsumerFactory() {
        JsonDeserializer<CorporateActionNotificationDto> deserializer = new JsonDeserializer<>(CorporateActionNotificationDto.class, objectMapper);
        deserializer.addTrustedPackages("ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto");
        return createConsumerFactory(kafkaProperties.getInternalNotificationOwnerBalance().getConsumer(), new StringDeserializer(), deserializer);
    }

    @Bean
    public ConsumerFactory<String, CorporateActionInstructionRequest> internalInstructionConsumerFactory() {
        JsonDeserializer<CorporateActionInstructionRequest> deserializer = new JsonDeserializer<>(CorporateActionInstructionRequest.class, objectMapper);
        deserializer.addTrustedPackages("ru.rshbintech.it_invest.backoffice.myinvestment.corp.action.dto");
        return createConsumerFactory(kafkaProperties.getInternalInstruction().getConsumer(), new StringDeserializer(), deserializer);
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(CustomKafkaProperties.Consumer consumerProps) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProps.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProps.getGroupId());
        props.putAll(consumerProps.getProperties());
        // Use deserializers from configuration
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                consumerProps.getKeyDeserializer() != null ?
                        consumerProps.getKeyDeserializer() : StringDeserializer.class);

        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                consumerProps.getValueDeserializer() != null ?
                        consumerProps.getValueDeserializer() : StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    private <T> ConsumerFactory<String, T> createConsumerFactory(CustomKafkaProperties.Consumer consumerProps, Deserializer<String> keyDesirializer, Deserializer<T> valueDesirializer) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerProps.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProps.getGroupId());
        props.putAll(consumerProps.getProperties());
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDesirializer);

        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDesirializer);

        return new DefaultKafkaConsumerFactory<>(props, keyDesirializer, valueDesirializer);
    }

    // Listener Container Factories
    @Bean(name = NOTIFICATION_FROM_DIASOFT_CONSUMER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, String> notificationFromDiasoftKafkaListenerContainerFactory() {
        return createListenerContainerFactory(notificationFromDiasoftConsumerFactory(),
                kafkaProperties.getNotificationFromDiasoft().getConsumer().getConcurrency());
    }

    @Bean(name = INTERNAL_INSTRUCTION_VIEW_CONSUMER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, CorporateActionInstructionRequest> instuctionViewKafkaListenerContainerFactory() {
        return createListenerContainerFactory(instructionViewConsumerFactory(),
                kafkaProperties.getInternalInstructionView().getConsumer().getConcurrency());
    }

    @Bean(name = INTERNAL_NOTIFICATION_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, CorporateActionNotificationDto> internalNotificationKafkaListenerContainerFactory() {
        return createListenerContainerFactory(internalNotificationConsumerFactory(),
                kafkaProperties.getInternalNotification().getConsumer().getConcurrency());
    }

    @Bean(name = INTERNAL_NOTIFICATION_OWNER_BALANCE)
    public ConcurrentKafkaListenerContainerFactory<String, CorporateActionNotificationDto> internalNotificationOwnerBalanceKafkaListenerContainerFactory() {
        return createListenerContainerFactory(internalNotificationConsumerFactory(),
                kafkaProperties.getInternalNotificationOwnerBalance().getConsumer().getConcurrency());
    }

    @Bean(name = INTERNAL_INSTRUCTION_CONSUMER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, CorporateActionInstructionRequest> internalInstructionKafkaListenerContainerFactory() {
        return createListenerContainerFactory(internalInstructionConsumerFactory(),
                kafkaProperties.getInternalInstruction().getConsumer().getConcurrency());
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> createListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory, int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(concurrency);
        return factory;
    }

    @Bean
    public ProducerFactory<String, CorporateActionNotificationDto> internalNotificationProducerFactory() {
        return createProducerFactory(kafkaProperties.getInternalNotification().getProducer());
    }

    @Bean
    public ProducerFactory<String, Object> internalInstructionProducerFactory() {
        return createProducerFactory(kafkaProperties.getInternalInstruction().getProducer());
    }

    @Bean
    public ProducerFactory<String, Object> instructionToDiasoftProducerFactory() {
        return createProducerFactory(kafkaProperties.getInstructionToDiasoft().getProducer());
    }

    private <T> ProducerFactory<String, T> createProducerFactory(CustomKafkaProperties.Producer producerProps) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producerProps.getBootstrapServers());
        if (producerProps.getSecurity() != null && producerProps.getSecurity().getProtocol() != null) {
            props.put("security.protocol", producerProps.getSecurity().getProtocol());
        }

        if (producerProps.getProducer() != null) {
            props.put(ProducerConfig.RETRIES_CONFIG, producerProps.getProducer().getRetries());
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    producerProps.getProducer().getKeySerializer() != null ?
                            producerProps.getProducer().getKeySerializer() : StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    producerProps.getProducer().getValueSerializer() != null ?
                            producerProps.getProducer().getValueSerializer() : JsonSerializer.class);
            props.put(ProducerConfig.ACKS_CONFIG, producerProps.getProducer().getAcks());
            props.putAll(producerProps.getProducer().getProperties());
        }

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, CorporateActionNotificationDto> internalNotificationKafkaTemplate() {
        return new KafkaTemplate(internalNotificationProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, CorporateActionInstructionRequest> internalInstructionKafkaTemplate() {
        return new KafkaTemplate(internalInstructionProducerFactory());
    }

    @Bean
    public KafkaTemplate<String, SendCorpActionsAssignmentReq> instructionToDiasoftKafkaTemplate() {
        return new KafkaTemplate(instructionToDiasoftProducerFactory());
    }
}