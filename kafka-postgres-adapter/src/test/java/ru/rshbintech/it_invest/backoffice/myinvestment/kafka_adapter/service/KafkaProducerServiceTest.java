package ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.config.KafkaConfig;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.exception.MessageSendException;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.KafkaMessage;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.model.RequestStatus;
import ru.rshbintech.it_invest.backoffice.myinvestment.kafka_adapter.property.KafkaProperties;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Юнит-тесты для {@link KafkaProducerService}
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Тестирование сервиса продюсера Kafka")
class KafkaProducerServiceTest {

    @Mock
    private KafkaProperties kafkaProperties;

    @Mock
    private KafkaConfig.KafkaTemplateFactory kafkaTemplateFactory;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaProducerService kafkaProducerService;

    private ObjectMapper objectMapper;
    private KafkaMessage testMessage;
    private RequestStatus testRequestStatus;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        kafkaProducerService = new KafkaProducerService(kafkaProperties, objectMapper, kafkaTemplateFactory);

        testMessage = KafkaMessage.builder()
                .topic("test-topic")
                .messageId("test-message-id")
                .headers("{}")
                .payload("test-payload")
                .build();

        testRequestStatus = new RequestStatus(
                RequestStatus.StatusValue.OK,
                "Success",
                "test-request-id"
        );

        lenient().when(kafkaTemplateFactory.createKafkaTemplate(anyString())).thenReturn(kafkaTemplate);
    }

    @Test
    @DisplayName("Успешная отправка сообщения")
    void sendMessage_Success() throws Exception {
        // Given
        when(kafkaProperties.getProducers()).thenReturn(Collections.singletonList(
                createProducerConfig("test-topic", "test-servers:9092")
        ));

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.complete(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        // When
        assertDoesNotThrow(() -> kafkaProducerService.sendMessage(testMessage));

        // Then
        verify(kafkaTemplate, times(1)).send("test-topic", "test-message-id", "test-payload");
    }

    @Test
    @DisplayName("Отправка сообщения с неподдерживаемым топиком")
    void sendMessage_UnsupportedTopic() {
        // Given
        when(kafkaProperties.getProducers()).thenReturn(Collections.emptyList());

        // When & Then
        MessageSendException exception = assertThrows(
                MessageSendException.class,
                () -> kafkaProducerService.sendMessage(testMessage)
        );
        assertTrue(exception.getCause().getMessage().contains("No producer configuration for topic"));
    }

    @Test
    @DisplayName("Успешная отправка статуса запроса")
    void sendRequestStatus_Success() throws Exception {
        // Given
        when(kafkaProperties.getProducers()).thenReturn(Collections.singletonList(
                createProducerConfig("request-status", "test-servers:9092")
        ));

        CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
        future.complete(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        // When
        assertDoesNotThrow(() -> kafkaProducerService.sendRequestStatus(testRequestStatus, "request-status"));

        // Then
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), anyString());
    }


    private KafkaProperties.ProducerConfig createProducerConfig(String topic, String bootstrapServers) {
        KafkaProperties.ProducerConfig config = new KafkaProperties.ProducerConfig();
        config.setTopic(topic);
        config.setBootstrapServers(bootstrapServers);
        return config;
    }
}
